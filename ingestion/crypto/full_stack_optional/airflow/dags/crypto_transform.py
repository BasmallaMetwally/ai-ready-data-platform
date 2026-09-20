"""Transformation: dbt build over the warehouse, plus a freshness audit.

Scheduled by **Dataset**, not by clock. It runs the moment
``crypto_market_ingest`` publishes new warehouse data — whether that is the
01:00 daily run, a manual trigger, or the tail of a six-year backfill. There is
no sleep-and-hope offset to tune, and no chance of building marts on data that
has not landed yet.

dbt is split into three tasks rather than one ``dbt build`` so that a failure
tells you *which stage* broke without reading a log: a source-freshness failure
is an upstream problem, a model failure is a SQL problem, and a test failure is
a data problem. They have genuinely different owners.
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline.config import get_settings
from pipeline.load import warehouse
from pipeline.observability import Alert, on_failure_callback, send_alert

log = logging.getLogger(__name__)
SETTINGS = get_settings()

# AIP-60 requires postgres:// Dataset URIs in the exact shape
# postgres://<host>/<database>/<schema>/<table> — found by actually importing
# the DAG, which surfaced a UserWarning (a hard error in Airflow 3) on the
# previous three-segment form. ANALYTICS_MARTS names fct_ohlcv_daily as its
# table because AIP-60 requires exactly one, but it stands in for "the whole
# star schema was rebuilt": every mart in this project rebuilds together in
# one dbt build, so no consumer should subscribe to a narrower dataset than this.
WAREHOUSE_OHLCV = Dataset("postgres://postgres/market/raw/binance_klines")
ANALYTICS_MARTS = Dataset("postgres://postgres/market/analytics/fct_ohlcv_daily")

DBT = "cd /opt/airflow/dbt && dbt"
DBT_FLAGS = "--profiles-dir . --target prod"


@dag(
    dag_id="crypto_transform",
    description="dbt build of the analytics star schema, triggered by new warehouse data",
    schedule=[WAREHOUSE_OHLCV],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # dbt writes to shared tables; never run two builds at once
    default_args={
        "owner": "analytics-engineering",
        "retries": 1,  # a failing SQL model will fail identically on retry
        "retry_delay": dt.timedelta(minutes=2),
        "execution_timeout": dt.timedelta(minutes=30),
        "on_failure_callback": on_failure_callback,
    },
    tags=["crypto", "dbt", "transformation"],
    doc_md=__doc__,
)
def crypto_transform():
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness {DBT_FLAGS}",
        doc_md="Fails if raw tables are stale — an upstream problem, not a modelling one.",
    )

    run_models = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_FLAGS} --fail-fast",
        doc_md="Builds staging, intermediate and mart models in dependency order.",
    )

    test_models = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS} --store-failures",
        doc_md=(
            "`--store-failures` persists failing rows into a dedicated schema so "
            "you can query exactly which records broke a test instead of guessing "
            "from a count."
        ),
    )

    generate_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate {DBT_FLAGS}",
        trigger_rule=TriggerRule.ALL_DONE,  # docs are still useful after a test failure
    )

    @task(outlets=[ANALYTICS_MARTS])
    def audit_freshness() -> dict:
        """Final gate: assert the marts actually moved."""
        with warehouse.connection(SETTINGS) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)                  AS row_count,
                       count(DISTINCT asset_key) AS assets,
                       max(date_key)             AS latest_date,
                       sum(case when has_unknown_asset then 1 else 0 end) AS unknown_rows
                FROM analytics.fct_ohlcv_daily
                """
            )
            row_count, assets, latest, unknown_rows = cur.fetchone()

        staleness = (dt.date.today() - latest).days if latest else 999
        summary = {
            "row_count": row_count,
            "assets": assets,
            "latest_date": str(latest),
            "staleness_days": staleness,
            "rows_with_unknown_asset": unknown_rows,
        }

        # An orphaned fact is not fatal — the unknown member keeps the star
        # schema intact — but it does mean the dimension is missing an asset.
        if unknown_rows:
            send_alert(
                Alert(
                    severity="warning",
                    title="Facts resolved to the unknown asset member",
                    dag_id="crypto_transform",
                    task_id="audit_freshness",
                    logical_date=str(latest),
                    detail=(
                        f"{unknown_rows} fact rows have no matching dim_asset version. "
                        "Add the symbol to SYMBOL_TO_COIN_ID and re-run crypto_assets_weekly."
                    ),
                )
            )

        if staleness > 2:
            raise ValueError(f"fct_ohlcv_daily is {staleness} days stale: {summary}")

        log.info("freshness audit passed: %s", summary)
        return summary

    source_freshness >> run_models >> test_models >> audit_freshness() >> generate_docs


crypto_transform()
