"""Bronze layer: land raw extracts in the object store, untouched.

Writing the immutable raw payload before any transformation is what makes the
pipeline replayable. If a business rule changes next quarter we re-run Spark
over bronze instead of re-hitting the API for three years of history.
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)

BRONZE_SCHEMA = pa.schema(
    [
        # `nullable=False` is deliberate and load-bearing, with one nuance worth
        # being precise about: `pa.Table.from_pylist()` will still silently fill
        # a missing dict key with null even against a non-nullable field — the
        # constraint is enforced by the *parquet writer*, not at table
        # construction, and raises `ArrowInvalid` from `write_bronze` rather
        # than from `Table.from_pylist` itself. Found by writing a real test for
        # a record missing "close" and checking where the error actually
        # surfaced. The upstream OHLCV_SUITE already rejects nulls before this
        # point, but bronze is meant to be a hard boundary on its own, not just
        # downstream of one specific caller remembering to validate first.
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval", pa.string(), nullable=False),
        pa.field("open_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("close_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("quote_volume", pa.float64(), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=False),
        # These two are genuinely optional: Binance can omit taker-side
        # breakdowns for illiquid pairs, so nullability here reflects reality
        # rather than an oversight.
        pa.field("taker_buy_base_volume", pa.float64(), nullable=True),
        pa.field("taker_buy_quote_volume", pa.float64(), nullable=True),
    ]
)


def _s3(settings: Settings) -> fs.S3FileSystem:
    endpoint = settings.s3_endpoint_url
    return fs.S3FileSystem(
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        endpoint_override=endpoint.replace("http://", "").replace("https://", ""),
        scheme="https" if endpoint.startswith("https") else "http",
        region=settings.s3_region,
        # pyarrow refuses create_dir() on a missing bucket unless this is set
        # explicitly — found by writing a real S3-backed test for ensure_bucket,
        # which raised OSError even after the FileType.NotFound check was fixed.
        allow_bucket_creation=True,
        allow_bucket_deletion=False,
    )


def _strip_scheme(path: str) -> str:
    return path.replace("s3a://", "").replace("s3://", "")


def write_bronze(
    records: list[dict[str, Any]],
    dataset: str,
    symbol: str,
    partition_date: str,
    settings: Settings | None = None,
) -> str:
    """Write one partition as a single parquet file and return its URI.

    The write is *overwrite by partition*: re-running the same logical date
    replaces the file rather than appending, which keeps retries idempotent.
    """
    settings = settings or get_settings()
    if not records:
        log.warning("no records for %s/%s on %s — skipping write", dataset, symbol, partition_date)
        return ""

    table = pa.Table.from_pylist(records, schema=BRONZE_SCHEMA)
    target_dir = _strip_scheme(settings.bronze_path(dataset, symbol, partition_date))
    target_file = f"{target_dir}/data.parquet"

    filesystem = _s3(settings)
    filesystem.create_dir(target_dir, recursive=True)
    with filesystem.open_output_stream(target_file) as sink:
        pq.write_table(table, sink, compression="snappy")

    log.info("wrote %s rows to s3a://%s", table.num_rows, target_file)
    return f"s3a://{target_file}"


def read_bronze(
    dataset: str,
    symbol: str,
    partition_date: str,
    settings: Settings | None = None,
) -> pa.Table:
    """Read a single partition back — used by data-quality checks and tests."""
    settings = settings or get_settings()
    path = _strip_scheme(settings.bronze_path(dataset, symbol, partition_date))
    return pq.read_table(path, filesystem=_s3(settings))


def ensure_bucket(settings: Settings | None = None) -> None:
    """Create the lake bucket if it does not exist yet (safe to call repeatedly).

    pyarrow's ``get_file_info`` does NOT raise for a missing path — it returns a
    FileInfo whose type is ``NotFound``. The previous try/except OSError version
    therefore never created anything and failed later with a confusing
    "no such bucket" deep inside a write.
    """
    settings = settings or get_settings()
    filesystem = _s3(settings)
    info = filesystem.get_file_info(settings.s3_bucket)
    if info.type == fs.FileType.NotFound:
        filesystem.create_dir(settings.s3_bucket)
        log.info("created bucket %s", settings.s3_bucket)
    else:
        log.debug("bucket %s already exists", settings.s3_bucket)
