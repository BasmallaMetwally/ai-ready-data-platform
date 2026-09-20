"""Tests for the shared HTTP layer: rate limiting and the circuit breaker."""

from __future__ import annotations

import time

import pytest
import requests
import responses

from pipeline.http import CircuitBreaker, CircuitOpenError, ResilientSession, TokenBucket


# ------------------------------------------------------------- token bucket
def test_bucket_allows_a_burst_without_waiting():
    bucket = TokenBucket(rate=10, capacity=5)
    waits = [bucket.acquire() for _ in range(5)]
    assert all(w == 0 for w in waits)


def test_bucket_throttles_once_the_burst_is_spent():
    bucket = TokenBucket(rate=20, capacity=2)
    bucket.acquire()
    bucket.acquire()
    started = time.monotonic()
    bucket.acquire()
    assert time.monotonic() - started >= 0.03  # ~1/20s replenishment


def test_bucket_refills_over_time():
    bucket = TokenBucket(rate=100, capacity=2)
    bucket.acquire(2)
    time.sleep(0.05)
    assert bucket.acquire() == 0  # refilled, so no wait


# ---------------------------------------------------------- circuit breaker
def test_breaker_stays_closed_below_the_threshold():
    breaker = CircuitBreaker(failure_threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    assert not breaker.is_open


def test_breaker_opens_at_the_threshold():
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.is_open


def test_success_resets_the_failure_count():
    breaker = CircuitBreaker(failure_threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert not breaker.is_open


def test_breaker_half_opens_after_the_reset_timeout():
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=0.05)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.is_open
    time.sleep(0.06)
    assert not breaker.is_open  # one probe allowed through


# ------------------------------------------------------------------ session
@responses.activate
def test_session_returns_successful_responses():
    responses.add(responses.GET, "https://example.com/data", json={"ok": True}, status=200)
    with ResilientSession(requests_per_second=1000, burst=10) as session:
        assert session.get("https://example.com/data").json() == {"ok": True}


@responses.activate
def test_server_errors_eventually_open_the_circuit():
    for _ in range(30):
        responses.add(responses.GET, "https://example.com/data", json={}, status=503)

    session = ResilientSession(
        requests_per_second=1000, burst=50, max_retries=0, failure_threshold=2
    )
    for _ in range(2):
        session.get("https://example.com/data")

    with pytest.raises(CircuitOpenError):
        session.get("https://example.com/data")


@responses.activate
def test_client_errors_do_not_trip_the_breaker():
    """A 404 means our request was wrong, not that the upstream is down."""
    for _ in range(10):
        responses.add(responses.GET, "https://example.com/data", json={}, status=404)

    session = ResilientSession(
        requests_per_second=1000, burst=50, max_retries=0, failure_threshold=2
    )
    for _ in range(5):
        session.get("https://example.com/data")
    assert not session.breaker.is_open


@responses.activate
def test_connection_errors_count_against_the_breaker():
    responses.add(responses.GET, "https://example.com/data", body=requests.ConnectionError("down"))
    session = ResilientSession(
        requests_per_second=1000, burst=50, max_retries=0, failure_threshold=1
    )
    with pytest.raises(requests.RequestException):
        session.get("https://example.com/data")
    assert session.breaker.is_open
