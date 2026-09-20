"""Shared HTTP plumbing for every API client.

Previously each extractor built its own session and CoinGecko reached into
``binance._build_session``, a private function in an unrelated module. Both
clients now depend on this one, which also adds the two protections a
production extractor needs and a bare ``requests.Session`` does not have:

* a **token-bucket rate limiter**, which throttles *before* the request rather
  than reacting to a 429 after the fact, and
* a **circuit breaker**, so a hard-down upstream fails tasks in milliseconds
  instead of burning the whole Airflow pool on retry backoff.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class CircuitOpenError(RuntimeError):
    """Raised while the breaker is open, without attempting a request."""


@dataclass
class TokenBucket:
    """Classic token bucket. Thread-safe, because Airflow's LocalExecutor runs
    mapped tasks in parallel processes that each hold their own client, and the
    same code path is used from Spark driver threads.

    Args:
        rate: tokens replenished per second.
        capacity: maximum burst size.
    """

    rate: float
    capacity: float
    _tokens: float = field(init=False)
    _last: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self._tokens = self.capacity
        self._last = time.monotonic()

    def acquire(self, tokens: float = 1.0) -> float:
        """Block until ``tokens`` are available. Returns seconds spent waiting."""
        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                deficit = (tokens - self._tokens) / self.rate
            time.sleep(deficit)
            waited += deficit


@dataclass
class CircuitBreaker:
    """Trips after ``failure_threshold`` consecutive failures, then refuses calls
    for ``reset_timeout`` seconds before allowing a single trial request."""

    failure_threshold: int = 5
    reset_timeout: float = 60.0
    _failures: int = field(default=0, init=False)
    _opened_at: float | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return False
            if time.monotonic() - self._opened_at >= self.reset_timeout:
                # Half-open: let exactly one request through to probe recovery.
                self._opened_at = None
                self._failures = self.failure_threshold - 1
                log.info("circuit breaker half-open, probing upstream")
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold and self._opened_at is None:
                self._opened_at = time.monotonic()
                log.error(
                    "circuit breaker opened after %s consecutive failures; "
                    "refusing requests for %ss",
                    self._failures,
                    self.reset_timeout,
                )


class ResilientSession:
    """A requests.Session wrapper applying retry, rate limiting and the breaker.

    Exposes ``get`` only; nothing in this pipeline writes to an upstream API,
    and keeping the surface small means the safety features cannot be bypassed
    by accident.
    """

    def __init__(
        self,
        *,
        requests_per_second: float = 8.0,
        burst: float = 16.0,
        max_retries: int = 5,
        failure_threshold: int = 5,
        user_agent: str = "crypto-market-pipeline/2.0",
    ) -> None:
        self.bucket = TokenBucket(rate=requests_per_second, capacity=burst)
        self.breaker = CircuitBreaker(failure_threshold=failure_threshold)
        self.session = self._build(max_retries, user_agent)

    @staticmethod
    def _build(max_retries: int, user_agent: str) -> requests.Session:
        retry = Retry(
            total=max_retries,
            backoff_factor=1.5,
            status_forcelist=(429, 418, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=16, pool_connections=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        return session

    def get(self, url: str, *, params: dict | None = None, timeout: int = 30) -> requests.Response:
        if self.breaker.is_open:
            raise CircuitOpenError(f"circuit is open; not calling {url}")

        waited = self.bucket.acquire()
        if waited > 0.5:
            log.debug("rate limiter delayed request by %.2fs", waited)

        try:
            response = self.session.get(url, params=params, timeout=timeout)
        except requests.RequestException:
            self.breaker.record_failure()
            raise

        # 5xx and rate-limit responses count against the breaker; 4xx generally
        # means *we* sent something wrong, so it should not trip it.
        if response.status_code >= 500 or response.status_code in (429, 418):
            self.breaker.record_failure()
        else:
            self.breaker.record_success()
        return response

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> ResilientSession:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
