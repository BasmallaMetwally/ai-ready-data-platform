"""Tests for the statistical checks.

These guard the subtler half of data quality: every row valid, dataset wrong.
"""

from __future__ import annotations

from pipeline.quality.checks import (
    Severity,
    check_row_count_anomaly,
    check_volume_anomaly,
    compute_baseline,
)

STABLE_HISTORY = [100, 102, 98, 101, 99, 103, 97, 100, 101, 99]


def test_baseline_needs_a_meaningful_sample():
    assert compute_baseline([100, 101, 99]) is None
    assert compute_baseline(STABLE_HISTORY) is not None


def test_baseline_ignores_nulls():
    baseline = compute_baseline([100, None, 102, 98, 101, 99, 103, 97])
    assert baseline is not None
    assert baseline.sample_size == 7


def test_baseline_statistics_are_correct():
    baseline = compute_baseline([10, 10, 10, 10, 10, 10, 10])
    assert baseline.mean == 10
    assert baseline.stddev == 0


def test_zero_variance_baseline_does_not_divide_by_zero():
    baseline = compute_baseline([10] * 8)
    assert baseline.z_score(10) == 0.0
    assert baseline.z_score(11) == float("inf")


def test_normal_row_count_passes():
    assert check_row_count_anomaly(101, STABLE_HISTORY).passed


def test_collapsed_row_count_is_flagged():
    """The failure mode this exists for: upstream returns a fraction of normal."""
    result = check_row_count_anomaly(3, STABLE_HISTORY)
    assert not result.passed
    assert "z=" in result.detail


def test_exploded_row_count_is_flagged():
    assert not check_row_count_anomaly(9000, STABLE_HISTORY).passed


def test_check_is_skipped_rather_than_failed_without_history():
    """A brand-new symbol must not fail its very first run."""
    result = check_row_count_anomaly(100, [])
    assert result.passed
    assert "insufficient history" in result.detail


def test_anomaly_checks_default_to_warn_not_error():
    """Statistical signals should never hard-stop a pipeline on their own."""
    assert check_row_count_anomaly(9000, STABLE_HISTORY).severity is Severity.WARN


def test_volume_anomaly_uses_quote_volume(valid_kline):
    history = [800_000_000.0] * 10
    assert check_volume_anomaly([valid_kline], history).passed
    collapsed = {**valid_kline, "quote_volume": 1.0}
    assert not check_volume_anomaly([collapsed], history).passed
