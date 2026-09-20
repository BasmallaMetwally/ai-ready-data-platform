"""Binance public market-data client.

Only the public REST endpoints are used, so no API key is required. The client
handles the two things that break naive extractors in production:

1. Pagination — Binance returns at most 1000 candles per call.
2. Rate limits — a 429/418 response carries a ``Retry-After`` header that must
   be respected or the IP gets banned.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterator
from dataclasses import asdict, dataclass

from pipeline.config import Settings, get_settings
from pipeline.http import ResilientSession

log = logging.getLogger(__name__)

MAX_LIMIT = 1000
"""Hard cap imposed by the Binance /klines endpoint."""


class BinanceError(RuntimeError):
    """Raised when Binance returns an unrecoverable error."""


@dataclass(frozen=True, slots=True)
class Kline:
    """One OHLCV candle, already normalised to sane Python types."""

    symbol: str
    interval: str
    open_time: dt.datetime
    close_time: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float

    def as_dict(self) -> dict:
        return asdict(self)


def _to_utc(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC)


class BinanceClient:
    """Thin, testable wrapper around the public klines endpoint."""

    def __init__(self, settings: Settings | None = None, session: ResilientSession | None = None):
        self.settings = settings or get_settings()
        # Binance allows 1200 request-weight per minute; a klines call costs 2.
        # 8 req/s leaves comfortable headroom for a parallel backfill.
        self.session = session or ResilientSession(
            requests_per_second=8.0,
            burst=16.0,
            max_retries=self.settings.http_max_retries,
        )

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> BinanceClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ public
    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> list[Kline]:
        """Return every closed candle in ``[start, end)``.

        The window is walked forward page by page. We advance the cursor past
        the last candle's ``open_time`` rather than by a fixed step, which keeps
        the loop correct across exchange downtime and missing candles.
        """
        if start >= end:
            raise ValueError(f"start ({start}) must be before end ({end})")

        rows = [k for page in self._paginate(symbol, interval, start, end) for k in page]
        # A candle whose close_time is in the future is still forming; drop it
        # so downstream models never see a partial bar.
        now = dt.datetime.now(tz=dt.UTC)
        closed = [k for k in rows if k.close_time <= now]
        log.info(
            "fetched %s candles for %s (%s) between %s and %s (%s dropped as unclosed)",
            len(closed),
            symbol,
            interval,
            start.date(),
            end.date(),
            len(rows) - len(closed),
        )
        return closed

    # ----------------------------------------------------------------- private
    def _paginate(
        self, symbol: str, interval: str, start: dt.datetime, end: dt.datetime
    ) -> Iterator[list[Kline]]:
        cursor_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        while cursor_ms < end_ms:
            payload = self._get(
                "/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor_ms,
                    "endTime": end_ms,
                    "limit": MAX_LIMIT,
                },
            )
            if not payload:
                return

            page = [self._parse_row(symbol, interval, row) for row in payload]
            yield page

            last_open_ms = int(payload[-1][0])
            if last_open_ms <= cursor_ms:  # no forward progress; bail out
                return
            cursor_ms = last_open_ms + 1

            if len(payload) < MAX_LIMIT:
                return
            # No sleep here: the token bucket in ResilientSession already paces
            # every call, so an extra fixed delay would only slow backfills down.

    def _get(self, path: str, params: dict) -> list:
        url = f"{self.settings.binance_base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.settings.http_timeout_seconds)

        if response.status_code in (429, 418):
            raise BinanceError(
                f"rate limited by Binance after {self.settings.http_max_retries} retries "
                f"(status {response.status_code})"
            )
        if not response.ok:
            raise BinanceError(f"GET {path} failed [{response.status_code}]: {response.text[:300]}")
        payload: list = response.json()
        return payload

    @staticmethod
    def _parse_row(symbol: str, interval: str, row: list) -> Kline:
        return Kline(
            symbol=symbol,
            interval=interval,
            open_time=_to_utc(int(row[0])),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            close_time=_to_utc(int(row[6])),
            quote_volume=float(row[7]),
            trade_count=int(row[8]),
            taker_buy_base_volume=float(row[9]),
            taker_buy_quote_volume=float(row[10]),
        )
