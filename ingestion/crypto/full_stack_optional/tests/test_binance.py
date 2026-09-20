"""Unit tests for the Binance extractor, fully mocked."""

from __future__ import annotations

import datetime as dt

import pytest
import responses

from pipeline.extract.binance import BinanceClient, BinanceError, Kline


@pytest.fixture
def window() -> tuple[dt.datetime, dt.datetime]:
    return (
        dt.datetime(2026, 9, 1, tzinfo=dt.UTC),
        dt.datetime(2026, 9, 3, tzinfo=dt.UTC),
    )


@responses.activate
def test_fetch_klines_parses_payload(settings, binance_payload, window):
    responses.add(
        responses.GET,
        "https://api.binance.com/api/v3/klines",
        json=binance_payload,
        status=200,
    )
    client = BinanceClient(settings)
    rows = client.fetch_klines("BTCUSDT", "1d", *window)

    assert len(rows) == 2
    first = rows[0]
    assert isinstance(first, Kline)
    assert first.symbol == "BTCUSDT"
    assert first.open == pytest.approx(64000.0)
    assert first.close == pytest.approx(65100.0)
    assert first.trade_count == 1_234_567
    assert first.open_time.tzinfo is dt.UTC


@responses.activate
def test_unclosed_candles_are_dropped(settings, window):
    """A candle whose close_time is in the future is still forming."""
    future_open = int((dt.datetime.now(tz=dt.UTC)).timestamp() * 1000)
    future_close = future_open + 86_400_000
    responses.add(
        responses.GET,
        "https://api.binance.com/api/v3/klines",
        json=[[future_open, "1", "2", "0.5", "1.5", "10", future_close, "15", 3, "5", "7", "0"]],
        status=200,
    )
    rows = BinanceClient(settings).fetch_klines("BTCUSDT", "1d", *window)
    assert rows == []


@responses.activate
def test_empty_payload_terminates_pagination(settings, window):
    responses.add(responses.GET, "https://api.binance.com/api/v3/klines", json=[], status=200)
    assert BinanceClient(settings).fetch_klines("BTCUSDT", "1d", *window) == []


@responses.activate
def test_server_error_raises_after_retries(settings, window):
    for _ in range(8):
        responses.add(
            responses.GET,
            "https://api.binance.com/api/v3/klines",
            json={"code": -1003, "msg": "Too many requests"},
            status=429,
        )
    settings.http_max_retries = 1
    with pytest.raises(BinanceError, match="rate limited"):
        BinanceClient(settings).fetch_klines("BTCUSDT", "1d", *window)


def test_inverted_window_is_rejected(settings):
    start = dt.datetime(2026, 9, 3, tzinfo=dt.UTC)
    end = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)
    with pytest.raises(ValueError, match="must be before"):
        BinanceClient(settings).fetch_klines("BTCUSDT", "1d", start, end)
