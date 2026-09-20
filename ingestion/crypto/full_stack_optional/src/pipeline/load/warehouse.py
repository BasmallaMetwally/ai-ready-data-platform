"""Warehouse layer: load the curated silver output into PostgreSQL.

Strategy is ``COPY`` into an unlogged temp table followed by ``INSERT ... ON
CONFLICT DO UPDATE``. That is an order of magnitude faster than row-by-row
inserts and, crucially, it is idempotent: re-running any logical date produces
the same table state, which is what lets Airflow retry a task safely.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql

from pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)

KLINE_COLUMNS: tuple[str, ...] = (
    "symbol",
    "interval",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)
KLINE_KEY: tuple[str, ...] = ("symbol", "interval", "open_time")

ASSET_COLUMNS: tuple[str, ...] = (
    "coin_id",
    "symbol",
    "trading_pair",
    "name",
    "market_cap_usd",
    "market_cap_rank",
    "circulating_supply",
    "max_supply",
    "all_time_high_usd",
    "all_time_high_date",
    "extracted_at",
)
ASSET_KEY: tuple[str, ...] = ("coin_id", "extracted_at")


@contextmanager
def connection(settings: Settings | None = None) -> Iterator[psycopg.Connection]:
    settings = settings or get_settings()
    conn = psycopg.connect(settings.pg_dsn, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _to_csv_buffer(rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> io.StringIO:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for row in rows:
        writer.writerow(["" if row.get(c) is None else row.get(c) for c in columns])
    buffer.seek(0)
    return buffer


def upsert(
    rows: Sequence[dict[str, Any]],
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    settings: Settings | None = None,
) -> int:
    """COPY ``rows`` into ``schema.table``, updating on primary-key conflict."""
    settings = settings or get_settings()
    if not rows:
        log.warning("upsert into %s called with zero rows — nothing to do", table)
        return 0

    schema = settings.pg_raw_schema
    target = sql.Identifier(schema, table)
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    key_idents = sql.SQL(", ").join(sql.Identifier(c) for c in key)
    updates = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
        for c in columns
        if c not in key
    )

    with connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TEMP TABLE staging_load (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(target=target)
        )
        copy_stmt = sql.SQL("COPY staging_load ({cols}) FROM STDIN WITH (FORMAT csv)").format(
            cols=col_idents
        )
        with cur.copy(copy_stmt) as copy:
            copy.write(_to_csv_buffer(rows, columns).read())

        cur.execute(
            sql.SQL(
                "INSERT INTO {target} ({cols}) SELECT {cols} FROM staging_load "
                "ON CONFLICT ({key}) DO UPDATE SET {updates}"
            ).format(target=target, cols=col_idents, key=key_idents, updates=updates)
        )
        affected = cur.rowcount

    log.info("upserted %s rows into %s.%s", affected, schema, table)
    return affected


def upsert_klines(rows: Sequence[dict[str, Any]], settings: Settings | None = None) -> int:
    return upsert(rows, "binance_klines", KLINE_COLUMNS, KLINE_KEY, settings)


def upsert_assets(rows: Sequence[dict[str, Any]], settings: Settings | None = None) -> int:
    return upsert(rows, "coingecko_assets", ASSET_COLUMNS, ASSET_KEY, settings)


def apply_ddl(ddl_path: str, settings: Settings | None = None) -> None:
    """Run a .sql file — used by the bootstrap task to create schemas/tables."""
    with open(ddl_path, encoding="utf-8") as handle:
        statements = handle.read()
    with connection(settings) as conn, conn.cursor() as cur:
        cur.execute(statements)
    log.info("applied DDL from %s", ddl_path)
