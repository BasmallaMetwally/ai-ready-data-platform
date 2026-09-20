"""Weekly refresh of the asset dimension (CoinGecko reference data).

Kept separate from the price DAG on purpose: reference data changes slowly, has
a different SLA, and must not be able to fail the daily price load.
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task

from pipeline.config import get_settings
from pipeline.extract.coingecko import CoinGeckoClient
from pipeline.load import warehouse
from pipeline.observability import audited, on_failure_callback

log = logging.getLogger(__name__)
SETTINGS = get_settings()

# AIP-60 requires postgres:// Dataset URIs as postgres://<host>/<db>/<schema>/<table>.
WAREHOUSE_OHLCV = Dataset("postgres://postgres/market/raw/binance_klines")


@dag(
    dag_id="crypto_assets_weekly",
    description="CoinGecko asset profiles -> raw.coingecko_assets -> dim_asset (SCD2)",
    schedule="0 3 * * 1",  # Mondays 03:00 UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=10),
        "on_failure_callback": on_failure_callback,
    },
    tags=["crypto", "reference-data", "scd2"],
    doc_md=__doc__,
)
def crypto_assets_weekly():
    @task(outlets=[WAREHOUSE_OHLCV])
    def extract_profiles(**context) -> int:
        """Land a new reference snapshot and publish the dataset.

        Publishing WAREHOUSE_OHLCV here rather than running dbt inline means the
        dimension rebuild goes through the same crypto_transform DAG as
        everything else. One place owns the marts, so there is no way for two
        DAGs to build dim_asset concurrently and race each other.
        """
        with audited("extract_profiles", context) as audit:
            client = CoinGeckoClient(SETTINGS)
            profiles = client.fetch_asset_profiles(SETTINGS.symbols)
            if not profiles:
                raise ValueError("CoinGecko returned no asset profiles")

            rows = [p.as_dict() for p in profiles]
            missing_rank = [r["coin_id"] for r in rows if r["market_cap_rank"] is None]
            if missing_rank:
                log.warning("no market-cap rank for %s", missing_rank)

            affected = warehouse.upsert_assets(rows, SETTINGS)
            audit["row_count"] = affected
        return affected

    extract_profiles()


crypto_assets_weekly()
