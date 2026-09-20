"""Integration tests for the warehouse loader, against a real PostgreSQL server.

These are the tests the v1 suite was missing: ``warehouse.py`` had 45%
coverage because the COPY-into-temp-table-then-upsert path cannot be verified
by mocking a cursor — the interesting behaviour (idempotent re-runs, CHECK
constraints, ON CONFLICT semantics) only exists in a real database.

Skipped automatically wherever no PostgreSQL server is reachable, so the fast
unit suite (`pytest tests/ -k "not integration"`) still needs nothing but pip.
Marked ``integration`` per pyproject's marker registration.
"""

from __future__ import annotations

import datetime as dt
import os

import psycopg
import pytest

from pipeline.config import Settings
from pipeline.load import warehouse

pytestmark = pytest.mark.integration


def _server_available(dsn: str) -> bool:
    try:
        with psycopg.connect(dsn, connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


TEST_DSN = (
    f"postgresql://{os.getenv('TEST_PG_USER', 'postgres')}:"
    f"{os.getenv('TEST_PG_PASSWORD', 'testing')}@"
    f"{os.getenv('TEST_PG_HOST', 'localhost')}:5432/"
    f"{os.getenv('TEST_PG_DATABASE', 'test_market')}"
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _server_available(TEST_DSN),
        reason="no reachable PostgreSQL test server (set TEST_PG_* to point at one)",
    ),
]


@pytest.fixture
def db_settings() -> Settings:
    return Settings(
        pg_host=os.getenv("TEST_PG_HOST", "localhost"),
        pg_user=os.getenv("TEST_PG_USER", "postgres"),
        pg_password=os.getenv("TEST_PG_PASSWORD", "testing"),
        pg_database=os.getenv("TEST_PG_DATABASE", "test_market"),
        s3_endpoint_url="http://localhost:9000",
    )


@pytest.fixture(autouse=True)
def _clean_tables(db_settings):
    """Every test starts from an empty table, without paying for a container
    restart per test."""
    with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE raw.binance_klines, raw.coingecko_assets, raw.pipeline_audit")
        conn.commit()
    yield


def _kline(**overrides) -> dict:
    opened = dt.datetime(2026, 9, 1, tzinfo=dt.UTC)
    base = {
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
    base.update(overrides)
    return base


class TestUpsertKlines:
    def test_inserts_new_rows(self, db_settings):
        affected = warehouse.upsert_klines([_kline()], db_settings)
        assert affected == 1

        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT close FROM raw.binance_klines WHERE symbol = 'BTCUSDT'")
            assert cur.fetchone()[0] == pytest.approx(65100.0)

    def test_rerunning_the_same_row_updates_rather_than_duplicates(self, db_settings):
        """The idempotency claim, proven against a real ON CONFLICT clause."""
        warehouse.upsert_klines([_kline(close=65100.0)], db_settings)
        # An exchange correction that revises close upward, staying within the
        # existing high so the row keeps satisfying chk_klines_high.
        warehouse.upsert_klines([_kline(close=65400.0)], db_settings)

        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), max(close) FROM raw.binance_klines")
            count, close = cur.fetchone()
        assert count == 1
        assert close == pytest.approx(65400.0)

    def test_different_symbols_do_not_collide(self, db_settings):
        warehouse.upsert_klines([_kline(symbol="BTCUSDT"), _kline(symbol="ETHUSDT")], db_settings)
        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.binance_klines")
            assert cur.fetchone()[0] == 2

    def test_null_taker_fields_load_as_sql_null_not_the_string_none(self, db_settings):
        """Regression guard for the CSV/COPY boundary: Python's `None` must
        become an empty CSV field, which COPY reads as NULL — not the literal
        four characters "None"."""
        row = _kline(taker_buy_base_volume=None, taker_buy_quote_volume=None)
        warehouse.upsert_klines([row], db_settings)

        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT taker_buy_base_volume FROM raw.binance_klines")
            assert cur.fetchone()[0] is None

    def test_violates_the_high_is_maximum_check_constraint(self, db_settings):
        """The DDL's CHECK constraints are meant to catch what application code
        misses. Confirm they actually fire against a real server."""
        bad = _kline(high=100.0)  # high < close, violates chk_klines_high
        with pytest.raises(psycopg.errors.CheckViolation):
            warehouse.upsert_klines([bad], db_settings)

    def test_violates_the_positive_price_check_constraint(self, db_settings):
        with pytest.raises(psycopg.errors.CheckViolation):
            warehouse.upsert_klines([_kline(open=-1.0)], db_settings)

    def test_a_failed_upsert_leaves_no_partial_rows(self, db_settings):
        """The whole batch runs in one transaction: a bad row in a batch of
        many must not leave the good rows committed halfway."""
        good = _kline(symbol="BTCUSDT")
        bad = _kline(symbol="ETHUSDT", low=999999.0)  # low > high, violates chk_klines_low
        with pytest.raises(psycopg.errors.CheckViolation):
            warehouse.upsert_klines([good, bad], db_settings)

        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.binance_klines")
            assert cur.fetchone()[0] == 0

    def test_empty_input_is_a_documented_noop(self, db_settings, caplog):
        with caplog.at_level("WARNING"):
            affected = warehouse.upsert_klines([], db_settings)
        assert affected == 0
        assert "nothing to do" in caplog.text


class TestUpsertAssets:
    def test_inserts_and_updates_on_conflict(self, db_settings):
        snapshot_1 = {
            "coin_id": "bitcoin",
            "symbol": "BTC",
            "trading_pair": "BTCUSDT",
            "name": "Bitcoin",
            "market_cap_usd": 1_000_000_000.0,
            "market_cap_rank": 1,
            "circulating_supply": 19_000_000.0,
            "max_supply": 21_000_000.0,
            "all_time_high_usd": 108000.0,
            "all_time_high_date": dt.date(2025, 1, 20),
            "extracted_at": dt.datetime(2026, 9, 1, tzinfo=dt.UTC),
        }
        warehouse.upsert_assets([snapshot_1], db_settings)

        snapshot_1_revised = {**snapshot_1, "market_cap_rank": 2}
        warehouse.upsert_assets([snapshot_1_revised], db_settings)

        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), max(market_cap_rank) FROM raw.coingecko_assets")
            count, rank = cur.fetchone()
        assert count == 1  # same (coin_id, extracted_at) -> update, not a new row
        assert rank == 2

    def test_same_coin_different_extraction_time_is_a_new_row(self, db_settings):
        """The primary key includes extracted_at specifically so history
        accumulates instead of being overwritten week over week."""
        base = {
            "coin_id": "bitcoin",
            "symbol": "BTC",
            "trading_pair": "BTCUSDT",
            "name": "Bitcoin",
            "market_cap_usd": 1e9,
            "market_cap_rank": 1,
            "circulating_supply": 19e6,
            "max_supply": 21e6,
            "all_time_high_usd": 108000.0,
            "all_time_high_date": dt.date(2025, 1, 20),
        }
        warehouse.upsert_assets(
            [{**base, "extracted_at": dt.datetime(2026, 9, 1, tzinfo=dt.UTC)}], db_settings
        )
        warehouse.upsert_assets(
            [{**base, "extracted_at": dt.datetime(2026, 9, 8, tzinfo=dt.UTC)}], db_settings
        )
        with psycopg.connect(db_settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.coingecko_assets")
            assert cur.fetchone()[0] == 2


class TestApplyDdl:
    def test_ddl_is_safe_to_reapply(self, db_settings, tmp_path):
        """The DDL file is run on every container start; it must tolerate
        already existing." objects, which is why every statement in it uses
        IF NOT EXISTS / OR REPLACE / a DO block guard.
        """
        ddl_path = tmp_path / "reapply.sql"
        ddl_path.write_text("CREATE SCHEMA IF NOT EXISTS raw;")
        warehouse.apply_ddl(str(ddl_path), db_settings)
        warehouse.apply_ddl(str(ddl_path), db_settings)  # must not raise
