"""Tests for the bronze-layer data-quality suite.

These matter more than they look: the suite is the only thing standing between
a malformed extract and the warehouse.
"""

from __future__ import annotations

import datetime as dt

import pytest

from pipeline.quality.checks import (
    OHLCV_SUITE,
    DataQualityError,
    Severity,
    check_no_date_gaps,
)


def test_valid_rows_pass(valid_kline):
    results = OHLCV_SUITE.run([valid_kline])
    assert all(r.passed for r in results)


def test_empty_extract_is_blocking():
    with pytest.raises(DataQualityError, match="dataset_not_empty"):
        OHLCV_SUITE.run([])


def test_duplicate_primary_key_is_blocking(valid_kline):
    with pytest.raises(DataQualityError, match="primary_key_is_unique"):
        OHLCV_SUITE.run([valid_kline, dict(valid_kline)])


def test_high_below_close_is_blocking(valid_kline):
    broken = {**valid_kline, "high": 100.0}
    with pytest.raises(DataQualityError, match="high_is_the_maximum"):
        OHLCV_SUITE.run([broken])


def test_negative_price_is_blocking(valid_kline):
    broken = {**valid_kline, "low": -5.0, "open": -1.0}
    with pytest.raises(DataQualityError):
        OHLCV_SUITE.run([broken])


def test_unclosed_candle_is_blocking(valid_kline):
    future = dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=2)
    with pytest.raises(DataQualityError, match="candle_is_already_closed"):
        OHLCV_SUITE.run([{**valid_kline, "close_time": future}])


def test_warn_severity_does_not_abort(valid_kline):
    """Taker volume above total volume is suspicious but must not fail the run."""
    suspicious = {**valid_kline, "taker_buy_base_volume": valid_kline["volume"] * 2}
    results = OHLCV_SUITE.run([suspicious])
    failed = [r for r in results if not r.passed]
    assert failed and all(r.severity is Severity.WARN for r in failed)


def test_missing_field_fails_closed(valid_kline):
    """A predicate that raises must count as a failure, never as a pass."""
    del valid_kline["high"]
    with pytest.raises(DataQualityError):
        OHLCV_SUITE.run([valid_kline])


def test_date_gap_detection(valid_kline):
    day2 = {**valid_kline, "open_time": valid_kline["open_time"] + dt.timedelta(days=1)}
    assert check_no_date_gaps([valid_kline, day2], expected_days=2).passed
    assert not check_no_date_gaps([valid_kline], expected_days=7).passed
