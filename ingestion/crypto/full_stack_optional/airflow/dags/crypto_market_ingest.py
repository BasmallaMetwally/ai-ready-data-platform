"""Daily ingestion: Binance OHLCV -> bronze lake -> silver -> warehouse.

This DAG now stops at the warehouse. Transformation lives in
``crypto_transform`` and is triggered by a **Dataset**, not by a schedule.

Why split it? Previously one DAG ran extract, Spark and dbt in sequence. That
meant a dbt model change required re-running extraction to test it, the backfill
DAG could not reuse the transformation step, and the retry semantics of an API
call and a SQL build — very different things — were forced to share one
``default_args``. Data-aware scheduling gives us one DAG per concern while
keeping them correctly ordered: ``crypto_transform`` starts when, and only when,
this DAG publishes ``WAREHOUSE_OHLCV``.

Design notes worth calling out in an interview:

* **Dynamic task mapping** (``.expand``) creates one extract task per symbol at
  runtime, so adding a trading pair is a Variable change, not a code change.
* **Idempotency**: every task converges to the same state on re-run, which is
  what makes ``catchup=True`` backfills safe.
* **The quality gate is a real gate.** If bronze fails validation the Spark and
  dbt tasks never start, so a bad extract cannot reach the marts.
* **Anomaly detection** compares each run against its own trailing baseline,
  catching the "every row valid, dataset still wrong" failure mode.
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pipeline.config import get_settings
from pipeline.extract.binance import BinanceClient
from pipeline.load import lake, warehouse
from pipeline.observability import audited, on_failure_callback, on_sla_miss_callback
from pipeline.quality.checks import (
    OHLCV_SUITE,
    check_no_date_gaps,
    check_row_count_anomaly,
    check_volume_anomaly,
    fetch_recent_row_counts,
)
from pipeline.quality.dq_bridge import run_ohlcv_quality_gate

log = logging.getLogger(__name__)
SETTINGS = get_settings()

# Datasets are the contract between DAGs. A downstream DAG declares a dependency
# on the URI; Airflow schedules it the moment an upstream task that lists it in
# `outlets` succeeds.
BRONZE_OHLCV = Dataset("s3://market-lake/bronze/binance_klines")
SILVER_OHLCV = Dataset("s3://market-lake/silver/ohlcv")
# AIP-60 requires postgres:// Dataset URIs as postgres://<host>/<db>/<schema>/<table>.
WAREHOUSE_OHLCV = Dataset("postgres://postgres/market/raw/binance_klines")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=30),
    "execution_timeout": dt.timedelta(minutes=45),
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,
}


def _symbols() -> list[str]:
    """Airflow Variable wins over the env default, so ops can change the universe
    without a redeploy."""
    raw = Variable.get("crypto_symbols", default_var=",".join(SETTINGS.symbols))
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


@dag(
    dag_id="crypto_market_ingest",
    description="Binance OHLCV -> S3 bronze -> Spark silver -> PostgreSQL",
    schedule="0 1 * * *",  # 01:00 UTC, comfortably after the 00:00 daily close
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss_callback,
    tags=["crypto", "elt", "spark", "ingestion"],
    doc_md=__doc__,
)
def crypto_market_ingest():
    @task
    def list_symbols() -> list[str]:
        symbols = _symbols()
        if not symbols:
            raise ValueError("crypto_symbols resolved to an empty list")
        log.info("ingesting %s symbols: %s", len(symbols), symbols)
        return symbols

    @task_group(group_id="bronze")
    def bronze_group(symbols):
        @task(
            retries=5,
            retry_delay=dt.timedelta(minutes=2),
            outlets=[BRONZE_OHLCV],
        )
        def extract_to_lake(symbol: str, **context) -> dict:
            """Pull a lookback window from Binance and land it as parquet.

            The window deliberately overlaps previous runs: exchanges revise
            candles, and the upsert downstream makes the overlap free.
            """
            logical_date = context["logical_date"]
            end = logical_date.add(days=1).start_of("day")
            start = end.subtract(days=SETTINGS.lookback_days)

            with audited(f"extract_{symbol}", context) as audit:
                with BinanceClient(SETTINGS) as client:
                    klines = client.fetch_klines(
                        symbol=symbol,
                        interval=SETTINGS.interval,
                        start=dt.datetime.fromtimestamp(start.timestamp(), tz=dt.UTC),
                        end=dt.datetime.fromtimestamp(end.timestamp(), tz=dt.UTC),
                    )
                if not klines:
                    raise AirflowSkipException(f"Binance returned no candles for {symbol}")

                rows = [k.as_dict() for k in klines]
                audit["row_count"] = len(rows)

                # 1. Deterministic row-level rules. Raises on ERROR severity.
                results = OHLCV_SUITE.run(rows)

                # 2. Completeness.
                results.append(check_no_date_gaps(rows, expected_days=SETTINGS.lookback_days))

                # 3. Statistical comparison against this task's own history.
                history = fetch_recent_row_counts("crypto_market_ingest", f"extract_{symbol}")
                results.append(check_row_count_anomaly(len(rows), history))
                results.append(check_volume_anomaly(rows, []))

                warnings = [str(r) for r in results if not r.passed]
                for warning in warnings:
                    log.warning("%s: %s", symbol, warning)

                # 4. Shared DQ score (0-100) via the same engine the e-commerce
                # ETL uses (see dq/quality_gate.py). This is a *second*, scored
                # opinion on top of the row-level checks above: OHLCV_SUITE
                # raises hard on individual bad rows, this scores the whole
                # batch and records a trend in quality_history.db so
                # `/quality/history/crypto.<SYMBOL>` shows it in the unified
                # API. It never blocks the DAG (raise_on_fail=False) — a low
                # score here is a signal, not (yet) a hard gate like the
                # ETL's DQ gate is for the warehouse tables.
                dq_result = run_ohlcv_quality_gate(rows, symbol=symbol, threshold=SETTINGS.dq_score_threshold)
                dq_score = dq_result.overall_score if dq_result is not None else None
                if dq_result is not None and not dq_result.passed:
                    log.warning(
                        "%s: DQ score %.1f is below threshold %.1f",
                        symbol, dq_result.overall_score, dq_result.threshold,
                    )
                audit["dq_score"] = dq_score

                uri = lake.write_bronze(
                    records=rows,
                    dataset="binance_klines",
                    symbol=symbol,
                    partition_date=logical_date.to_date_string(),
                    settings=SETTINGS,
                )

            return {
                "symbol": symbol, "row_count": len(rows), "uri": uri,
                "warnings": warnings, "dq_score": dq_score,
            }

        @task(sla=dt.timedelta(minutes=30))
        def summarise(results: list[dict]) -> str:
            """Gate for the whole bronze phase, not any one symbol.

            Airflow does not support `sla=` on a dynamically mapped task at
            all — found by actually running a `DagBag` import, which is what
            the "DAG import check" CI job is for. A per-symbol SLA would have
            been the wrong granularity anyway once several symbols run in
            parallel; "all of bronze finished within 30 minutes" is the
            meaningful threshold, and it can only be expressed on a task that
            depends on every mapped instance, which is exactly what this one
            already does.
            """
            total = sum(r["row_count"] for r in results)
            log.info("bronze complete: %s rows across %s symbols", total, len(results))
            if total == 0:
                raise ValueError("bronze layer produced no rows — refusing to continue")
            return ",".join(r["symbol"] for r in results)

        return summarise(extract_to_lake.expand(symbol=symbols))

    ingested_symbols = bronze_group(list_symbols())

    bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/src/pipeline/transform/spark_silver.py",
        conn_id="spark_default",
        name="bronze_to_silver_{{ ds }}",
        application_args=[
            "--date",
            "{{ ds }}",
            "--symbols",
            "{{ ti.xcom_pull(task_ids='bronze.summarise') }}",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.driver.memory": SETTINGS.spark_driver_memory,
            "spark.executor.memory": SETTINGS.spark_executor_memory,
            "spark.pyspark.python": "python3",
        },
        env_vars={"PYTHONPATH": "/opt/airflow/src"},
        outlets=[SILVER_OHLCV],
        verbose=False,
    )

    @task(outlets=[WAREHOUSE_OHLCV])
    def load_silver_to_warehouse(**context) -> int:
        """Read the silver partition and upsert it into Postgres.

        Deliberately *not* a Spark JDBC write: a COPY-based loader is faster at
        this volume and keeps the transactional upsert semantics we need.
        """
        import datetime as _dt

        import pyarrow.dataset as ds

        from pipeline.load.lake import _s3, _strip_scheme

        logical_date = context["logical_date"].date()
        cutoff = logical_date - _dt.timedelta(days=SETTINGS.lookback_days)

        with audited("load_silver_to_warehouse", context) as audit:
            dataset = ds.dataset(
                _strip_scheme(SETTINGS.silver_path("ohlcv")),
                filesystem=_s3(SETTINGS),
                format="parquet",
                partitioning="hive",
            )
            # Compare dates as dates. The previous version compared the hive
            # partition value against a string, which only worked by accident of
            # ISO-8601 sorting lexicographically the same as chronologically.
            table = dataset.to_table(filter=ds.field("trade_date") >= cutoff)

            keep = set(warehouse.KLINE_COLUMNS)
            trimmed = [{k: v for k, v in row.items() if k in keep} for row in table.to_pylist()]
            if not trimmed:
                raise ValueError(f"silver layer returned no rows on or after {cutoff}")

            affected = warehouse.upsert_klines(trimmed, SETTINGS)
            audit["row_count"] = affected

        log.info("loaded %s rows into the warehouse for %s", affected, logical_date)
        return affected

    ingested_symbols >> bronze_to_silver >> load_silver_to_warehouse()


crypto_market_ingest()
