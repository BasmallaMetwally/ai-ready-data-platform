"""Tests for the Spark bronze->silver transformation, against a real local
PySpark session — not mocked DataFrames.

``deduplicate``, ``enrich`` and ``quarantine_invalid`` take and return
DataFrames without touching S3, so they are testable in isolation from
``read_bronze``/``write_silver``. That boundary is exactly why the module is
structured this way, and these tests are what makes the boundary worth having.

Requires a JVM (``java`` on PATH); skipped automatically otherwise, so the
core unit suite has no such dependency.
"""

from __future__ import annotations

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

pytestmark = pytest.mark.integration

from pyspark.sql import Row, SparkSession  # noqa: E402

from pipeline.transform.spark_silver import (  # noqa: E402
    deduplicate,
    enrich,
    quarantine_invalid,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_spark_silver")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    opened = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)
    base = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "open_time": opened,
        "close_time": opened + dt.timedelta(hours=23, minutes=59),
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
    base.update(overrides)
    return base


class TestDeduplicate:
    def test_keeps_a_single_row_when_there_is_no_overlap(self, spark):
        df = spark.createDataFrame([Row(**_row())])
        assert deduplicate(df).count() == 1

    def test_collapses_overlapping_partitions_to_one_row_per_key(self, spark):
        """The daily DAG lands overlapping lookback windows on purpose; this is
        the function responsible for making that overlap harmless."""
        df = spark.createDataFrame([Row(**_row()), Row(**_row())])
        assert deduplicate(df).count() == 1

    def test_keeps_the_version_with_the_latest_close_time(self, spark):
        """An exchange revision arrives with a later close_time (re-ingested
        after the fact); it must win over the original."""
        original = _row(close=65100.0, close_time=dt.datetime(2026, 9, 1, 23, 59, tzinfo=dt.UTC))
        revised = _row(close=65200.0, close_time=dt.datetime(2026, 9, 2, 6, 0, tzinfo=dt.UTC))
        df = spark.createDataFrame([Row(**original), Row(**revised)])

        result = deduplicate(df).collect()
        assert len(result) == 1
        assert result[0].close == pytest.approx(65200.0)

    def test_different_symbols_are_independent(self, spark):
        df = spark.createDataFrame([Row(**_row(symbol="BTCUSDT")), Row(**_row(symbol="ETHUSDT"))])
        assert deduplicate(df).count() == 2

    def test_different_open_times_are_independent(self, spark):
        day1 = _row(open_time=dt.datetime(2026, 9, 1, tzinfo=dt.UTC))
        day2 = _row(open_time=dt.datetime(2026, 9, 2, tzinfo=dt.UTC))
        df = spark.createDataFrame([Row(**day1), Row(**day2)])
        assert deduplicate(df).count() == 2


class TestEnrich:
    def test_adds_the_expected_derived_columns(self, spark):
        df = spark.createDataFrame([Row(**_row())])
        result = enrich(df).collect()[0]
        for column in (
            "trade_date",
            "price_range",
            "range_pct",
            "intraday_return_pct",
            "is_green",
            "vwap_proxy",
            "ingested_at",
        ):
            assert hasattr(result, column)

    def test_price_range_is_high_minus_low(self, spark):
        df = spark.createDataFrame([Row(**_row(high=65500.0, low=63200.0))])
        result = enrich(df).collect()[0]
        assert float(result.price_range) == pytest.approx(2300.0)

    def test_is_green_reflects_close_versus_open(self, spark):
        up = spark.createDataFrame([Row(**_row(open=100.0, close=110.0))])
        down = spark.createDataFrame([Row(**_row(open=110.0, close=100.0))])
        assert enrich(up).collect()[0].is_green is True
        assert enrich(down).collect()[0].is_green is False

    def test_vwap_proxy_divides_quote_by_base_volume(self, spark):
        df = spark.createDataFrame([Row(**_row(quote_volume=1_000_000.0, volume=20.0))])
        result = enrich(df).collect()[0]
        assert float(result.vwap_proxy) == pytest.approx(50_000.0)

    def test_vwap_proxy_is_null_rather_than_dividing_by_zero(self, spark):
        df = spark.createDataFrame([Row(**_row(volume=0.0))])
        assert enrich(df).collect()[0].vwap_proxy is None

    def test_does_not_compute_cross_partition_lag_columns(self, spark):
        """Regression guard for the v2 fix: enrich() must stay row-local. A
        `prior_close`/`gap_pct` column reappearing here would mean the
        cross-partition lag() bug crept back in."""
        df = spark.createDataFrame([Row(**_row())])
        columns = set(enrich(df).columns)
        assert "prior_close" not in columns
        assert "gap_pct" not in columns


class TestQuarantineInvalid:
    def test_a_valid_row_is_not_quarantined(self, spark):
        df = enrich(spark.createDataFrame([Row(**_row())]))
        clean, quarantined = quarantine_invalid(df)
        assert clean.count() == 1
        assert quarantined.count() == 0

    def test_high_below_low_is_quarantined(self, spark):
        df = enrich(
            spark.createDataFrame([Row(**_row(high=50.0, low=60.0, open=55.0, close=55.0))])
        )
        clean, quarantined = quarantine_invalid(df)
        assert clean.count() == 0
        assert quarantined.count() == 1
        assert quarantined.collect()[0].quarantine_reason == "failed_ohlc_invariants"

    def test_negative_volume_is_quarantined(self, spark):
        df = enrich(spark.createDataFrame([Row(**_row(volume=-1.0))]))
        clean, quarantined = quarantine_invalid(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_zero_or_negative_open_is_quarantined(self, spark):
        df = enrich(spark.createDataFrame([Row(**_row(open=0.0))]))
        clean, _quarantined = quarantine_invalid(df)
        assert clean.count() == 0

    def test_valid_and_invalid_rows_are_correctly_separated(self, spark):
        good = _row(symbol="BTCUSDT")
        bad = _row(symbol="ETHUSDT", high=10.0, low=20.0, open=15.0, close=15.0)
        df = enrich(spark.createDataFrame([Row(**good), Row(**bad)]))
        clean, quarantined = quarantine_invalid(df)
        assert clean.collect()[0].symbol == "BTCUSDT"
        assert quarantined.collect()[0].symbol == "ETHUSDT"


class TestFullPipeline:
    def test_bronze_to_silver_end_to_end_on_an_in_memory_frame(self, spark):
        """Composes all three stages the way main() does, minus the S3 I/O."""
        rows = [
            Row(**_row(symbol="BTCUSDT")),
            Row(**_row(symbol="BTCUSDT")),  # duplicate from lookback overlap
            Row(**_row(symbol="ETHUSDT", high=1.0, low=2.0, open=1.5, close=1.5)),  # invalid
        ]
        df = spark.createDataFrame(rows)
        clean, quarantined = quarantine_invalid(enrich(deduplicate(df)))

        assert clean.count() == 1
        assert clean.collect()[0].symbol == "BTCUSDT"
        assert quarantined.count() == 1
