"""Spark job: bronze (raw parquet) -> silver (clean, deduplicated, enriched).

Run standalone via spark-submit, which is how the Airflow ``SparkSubmitOperator``
invokes it:

    spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        src/pipeline/transform/spark_silver.py --date 2026-09-16 --symbols BTCUSDT,ETHUSDT

Why Spark for a few thousand rows a day? Because the same job runs unchanged
over a full multi-year, minute-level backfill — tens of millions of candles —
where a pandas implementation would fall over.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipeline.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
log = logging.getLogger("spark_silver")

PRICE_TYPE = DecimalType(38, 12)
"""Money is never a float in the warehouse. Decimal(38,12) survives both
BTC-scale prices and eight-decimal altcoins without rounding drift."""


def build_spark(app_name: str = "bronze_to_silver") -> SparkSession:
    settings = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return builder.getOrCreate()


def read_bronze(spark: SparkSession, symbols: list[str], date: str) -> DataFrame:
    settings = get_settings()
    paths = [settings.bronze_path("binance_klines", s, date) for s in symbols]
    log.info("reading %s bronze partitions", len(paths))
    return spark.read.option(
        "basePath", f"s3a://{settings.s3_bucket}/bronze/binance_klines"
    ).parquet(*paths)


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep exactly one row per (symbol, interval, open_time).

    Bronze partitions overlap by design (``lookback_days``), so the same candle
    arrives several times. We keep the *latest* ingested version, which is the
    one reflecting any exchange correction.
    """
    window = Window.partitionBy("symbol", "interval", "open_time").orderBy(
        F.col("close_time").desc()
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def enrich(df: DataFrame) -> DataFrame:
    """Add derived measures that are correct within a single partition.

    DELIBERATELY NO lag()/lead() HERE.

    An earlier version computed prior_close and gap_pct with a window ordered by
    open_time. That is wrong: this job only reads the partitions for one logical
    date, so the first candle of every run has no predecessor in scope and its
    "prior close" comes back null — or worse, silently wrong after a backfill
    reads a different slice. The values looked plausible, which is exactly what
    makes the bug expensive.

    Anything requiring cross-day context is computed in dbt instead, where the
    window runs over the complete warehouse history. Silver's job is row-local
    cleaning and typing, nothing more.
    """
    return (
        df.withColumn("trade_date", F.to_date("open_time"))
        .withColumn("open", F.col("open").cast(PRICE_TYPE))
        .withColumn("high", F.col("high").cast(PRICE_TYPE))
        .withColumn("low", F.col("low").cast(PRICE_TYPE))
        .withColumn("close", F.col("close").cast(PRICE_TYPE))
        .withColumn("price_range", F.col("high") - F.col("low"))
        .withColumn(
            "range_pct",
            F.when(F.col("low") > 0, (F.col("high") - F.col("low")) / F.col("low") * 100),
        )
        .withColumn(
            "intraday_return_pct",
            F.when(F.col("open") > 0, (F.col("close") - F.col("open")) / F.col("open") * 100),
        )
        .withColumn("is_green", F.col("close") >= F.col("open"))
        .withColumn(
            "vwap_proxy",
            F.when(F.col("volume") > 0, F.col("quote_volume") / F.col("volume")).cast(PRICE_TYPE),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )


def quarantine_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split the frame into trustworthy rows and a quarantine table.

    Dropping bad rows silently is how data teams lose credibility. Everything
    rejected here is written to its own path so it can be inspected later.
    """
    is_valid = (
        F.col("open").isNotNull()
        & (F.col("open") > 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("high") >= F.col("close"))
        & (F.col("low") <= F.col("close"))
        & (F.col("volume") >= 0)
    )
    return df.filter(is_valid), df.filter(~is_valid).withColumn(
        "quarantine_reason", F.lit("failed_ohlc_invariants")
    )


def write_silver(df: DataFrame, quarantined: DataFrame) -> None:
    settings = get_settings()
    (
        df.repartition("symbol")
        .write.mode("overwrite")
        .partitionBy("symbol", "trade_date")
        .parquet(settings.silver_path("ohlcv"))
    )
    if quarantined.take(1):
        (
            quarantined.write.mode("append")
            .partitionBy("symbol")
            .parquet(settings.silver_path("_quarantine_ohlcv"))
        )
        log.warning("quarantined rows written to %s", settings.silver_path("_quarantine_ohlcv"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transform bronze klines into the silver layer")
    parser.add_argument("--date", required=True, help="logical date, YYYY-MM-DD")
    parser.add_argument("--symbols", required=True, help="comma-separated trading pairs")
    args = parser.parse_args(argv)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    spark = build_spark()
    try:
        bronze = read_bronze(spark, symbols, args.date)
        deduped = deduplicate(bronze)
        enriched = enrich(deduped)
        clean, quarantined = quarantine_invalid(enriched)

        clean_count, bad_count = clean.count(), quarantined.count()
        log.info("silver: %s valid rows, %s quarantined", clean_count, bad_count)
        if clean_count == 0:
            log.error("no valid rows produced for %s — failing the task", args.date)
            return 1

        write_silver(clean, quarantined)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
