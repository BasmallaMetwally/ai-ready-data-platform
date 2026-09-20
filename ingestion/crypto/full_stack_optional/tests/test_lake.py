"""Tests for the bronze-layer parquet writer, against a real S3-compatible server.

``pyarrow.fs.S3FileSystem`` is a native (non-boto3) implementation, so the usual
``moto.mock_aws`` decorator — which patches botocore — does not intercept it.
What does work, because it is a real HTTP server rather than a patch, is
``moto``'s standalone server mode. It speaks the S3 API the same way MinIO does
in production, so these tests exercise the actual `write_bronze` /
`read_bronze` code path, not a stand-in for it.
"""

from __future__ import annotations

import datetime as dt

import pyarrow.parquet as pq
import pytest

from pipeline.config import Settings
from pipeline.load import lake

moto_server = pytest.importorskip("moto.server", reason="moto[server] not installed")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def s3_server():
    """One moto server per test module — starting it per-test is unnecessarily slow."""
    server = moto_server.ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def lake_settings(s3_server) -> Settings:
    settings = Settings(
        s3_endpoint_url=s3_server,
        s3_access_key="testing",
        s3_secret_key="testing",
        s3_bucket="test-market-lake",
        pg_host="localhost",
    )
    lake.ensure_bucket(settings)
    return settings


@pytest.fixture
def sample_records() -> list[dict]:
    opened = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)
    return [
        {
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
    ]


def test_ensure_bucket_creates_a_missing_bucket(s3_server):
    """This exercises the exact fix from the v2 hardening round: get_file_info
    returns a NotFound FileInfo rather than raising, and a version that only
    checked `except OSError` silently never created anything."""
    settings = Settings(
        s3_endpoint_url=s3_server,
        s3_access_key="testing",
        s3_secret_key="testing",
        s3_bucket="brand-new-bucket",
        pg_host="localhost",
    )
    filesystem = lake._s3(settings)
    assert filesystem.get_file_info(settings.s3_bucket).type.name == "NotFound"

    lake.ensure_bucket(settings)

    assert filesystem.get_file_info(settings.s3_bucket).type.name != "NotFound"


def test_ensure_bucket_is_safe_to_call_twice(lake_settings):
    lake.ensure_bucket(lake_settings)  # already exists via the fixture; must not raise
    lake.ensure_bucket(lake_settings)


def test_write_bronze_round_trips_through_read_bronze(lake_settings, sample_records):
    uri = lake.write_bronze(
        sample_records, "binance_klines", "BTCUSDT", "2026-09-01", lake_settings
    )
    assert uri.startswith("s3a://")

    table = lake.read_bronze("binance_klines", "BTCUSDT", "2026-09-01", lake_settings)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["close"] == pytest.approx(65100.0)


def test_write_bronze_enforces_the_declared_schema(lake_settings, sample_records):
    """A record missing a required field must fail loudly at write time, not
    surface later as a silent null three layers downstream.

    The interesting detail this test pins down: `pa.Table.from_pylist` fills
    the missing key with null *without* raising, even against a schema field
    marked `nullable=False` — that constraint is enforced by the parquet
    writer, not at table construction. So the exception has to be caught
    around the full `write_bronze` call, not a bare `Table.from_pylist`.
    """
    import pyarrow as pa

    broken = [{k: v for k, v in sample_records[0].items() if k != "close"}]
    with pytest.raises(pa.lib.ArrowInvalid, match="non-nullable"):
        lake.write_bronze(broken, "binance_klines", "BTCUSDT", "2026-09-01", lake_settings)


def test_write_bronze_with_empty_records_is_a_documented_noop(lake_settings, caplog):
    with caplog.at_level("WARNING"):
        uri = lake.write_bronze([], "binance_klines", "BTCUSDT", "2026-09-01", lake_settings)
    assert uri == ""
    assert "skipping write" in caplog.text


def test_rerunning_the_same_partition_overwrites_rather_than_appends(lake_settings, sample_records):
    """Idempotency, at the storage layer: re-landing a partition must replace
    it, not duplicate it — this is what makes an Airflow retry safe."""
    lake.write_bronze(sample_records, "binance_klines", "BTCUSDT", "2026-09-01", lake_settings)
    doubled = sample_records + sample_records
    lake.write_bronze(doubled, "binance_klines", "BTCUSDT", "2026-09-01", lake_settings)

    table = lake.read_bronze("binance_klines", "BTCUSDT", "2026-09-01", lake_settings)
    assert table.num_rows == 2  # reflects the second write's contents, not 1 + 2


def test_bronze_path_is_hive_partitioned(lake_settings):
    path = lake_settings.bronze_path("binance_klines", "ETHUSDT", "2026-09-01")
    assert path == "s3a://test-market-lake/bronze/binance_klines/symbol=ETHUSDT/dt=2026-09-01"


def test_written_file_is_valid_parquet_with_snappy_compression(lake_settings, sample_records):
    """Guards the on-disk format contract Spark's reader depends on."""
    lake.write_bronze(sample_records, "binance_klines", "BTCUSDT", "2026-09-01", lake_settings)

    target = lake._strip_scheme(
        lake_settings.bronze_path("binance_klines", "BTCUSDT", "2026-09-01")
    )
    filesystem = lake._s3(lake_settings)
    with filesystem.open_input_file(f"{target}/data.parquet") as handle:
        metadata = pq.ParquetFile(handle).metadata
    assert metadata.row_group(0).column(0).compression == "SNAPPY"
