"""Manually triggered historical backfill.

Trigger with a config payload:

    {"start_date": "2020-01-01", "end_date": "2026-09-01",
     "symbols": "BTCUSDT,ETHUSDT", "chunk_days": 365}

The date range is split into chunks and mapped across parallel tasks, so a
six-year backfill is bounded by pool size rather than by a single long task
that loses all its progress on one timeout.
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pipeline.config import get_settings
from pipeline.extract.binance import BinanceClient
from pipeline.load import lake
from pipeline.quality.checks import OHLCV_SUITE

log = logging.getLogger(__name__)
SETTINGS = get_settings()


@dag(
    dag_id="crypto_backfill",
    description="Parallel historical backfill of Binance OHLCV into the bronze layer",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=6,  # respect the exchange rate limit
    params={
        "start_date": Param("2023-01-01", type="string", format="date"),
        "end_date": Param("2026-09-01", type="string", format="date"),
        "symbols": Param("BTCUSDT,ETHUSDT", type="string"),
        "chunk_days": Param(365, type="integer", minimum=30, maximum=1000),
    },
    default_args={"owner": "data-engineering", "retries": 3},
    tags=["crypto", "backfill", "manual"],
    doc_md=__doc__,
)
def crypto_backfill():
    @task
    def plan_chunks(**context) -> list[dict]:
        """Turn (range x symbols) into a flat list of independent work units."""
        params = context["params"]
        start = dt.date.fromisoformat(params["start_date"])
        end = dt.date.fromisoformat(params["end_date"])
        chunk = dt.timedelta(days=int(params["chunk_days"]))
        symbols = [s.strip().upper() for s in params["symbols"].split(",") if s.strip()]

        units: list[dict] = []
        for symbol in symbols:
            cursor = start
            while cursor < end:
                stop = min(cursor + chunk, end)
                units.append(
                    {"symbol": symbol, "start": cursor.isoformat(), "end": stop.isoformat()}
                )
                cursor = stop
        log.info("planned %s work units across %s symbols", len(units), len(symbols))
        return units

    @task(max_active_tis_per_dag=4, retries=4, retry_delay=dt.timedelta(minutes=3))
    def backfill_chunk(unit: dict) -> dict:
        start = dt.datetime.fromisoformat(unit["start"]).replace(tzinfo=dt.UTC)
        end = dt.datetime.fromisoformat(unit["end"]).replace(tzinfo=dt.UTC)

        with BinanceClient(SETTINGS) as client:
            klines = client.fetch_klines(unit["symbol"], SETTINGS.interval, start, end)
        if not klines:
            return {"symbol": unit["symbol"], "rows": 0}

        rows = [k.as_dict() for k in klines]
        OHLCV_SUITE.run(rows)

        # Land each chunk under the partition of its first candle so the silver
        # job can discover it with the same Hive layout the daily DAG uses.
        by_day: dict[str, list[dict]] = {}
        for row in rows:
            by_day.setdefault(row["open_time"].date().isoformat(), []).append(row)
        for day, day_rows in by_day.items():
            lake.write_bronze(day_rows, "binance_klines", unit["symbol"], day, SETTINGS)

        return {"symbol": unit["symbol"], "rows": len(rows), "partitions": len(by_day)}

    @task
    def report(results: list[dict]) -> dict:
        total = sum(r["rows"] for r in results)
        log.info("backfill complete: %s rows in %s chunks", total, len(results))
        return {"total_rows": total, "chunks": len(results)}

    summary = report(backfill_chunk.expand(unit=plan_chunks()))

    rebuild_silver = SparkSubmitOperator(
        task_id="rebuild_silver",
        application="/opt/airflow/src/pipeline/transform/spark_silver.py",
        conn_id="spark_default",
        application_args=["--date", "*", "--symbols", "{{ params.symbols }}"],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        env_vars={"PYTHONPATH": "/opt/airflow/src"},
    )

    summary >> rebuild_silver


crypto_backfill()
