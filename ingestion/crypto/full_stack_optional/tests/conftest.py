"""Shared fixtures. Deliberately no network and no database — unit tests must
run in milliseconds on a laptop and in CI."""

from __future__ import annotations

import datetime as dt

import pytest

from pipeline.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        pg_host="localhost",
        s3_endpoint_url="http://localhost:9000",
        symbols=["BTCUSDT"],
        lookback_days=2,
    )


@pytest.fixture
def valid_kline() -> dict:
    opened = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)
    return {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "open_time": opened,
        "close_time": opened + dt.timedelta(days=1) - dt.timedelta(milliseconds=1),
        "open": 64000.0,
        "high": 65500.0,
        "low": 63200.0,
        "close": 65100.0,
        "volume": 12345.678,
        "quote_volume": 800_000_000.0,
        "trade_count": 1_234_567,
        "taker_buy_base_volume": 6000.0,
        "taker_buy_quote_volume": 390_000_000.0,
    }


@pytest.fixture
def binance_payload() -> list[list]:
    """Raw Binance /klines response shape: an array of 12-element arrays."""
    return [
        [
            1756684800000,
            "64000.00",
            "65500.00",
            "63200.00",
            "65100.00",
            "12345.678",
            1756771199999,
            "800000000.00",
            1234567,
            "6000.00",
            "390000000.00",
            "0",
        ],
        [
            1756771200000,
            "65100.00",
            "66800.00",
            "64900.00",
            "66500.00",
            "9876.543",
            1756857599999,
            "650000000.00",
            987654,
            "5000.00",
            "330000000.00",
            "0",
        ],
    ]
