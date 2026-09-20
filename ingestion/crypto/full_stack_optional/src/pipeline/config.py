"""Central configuration, loaded once from the environment.

Every component (Airflow tasks, Spark jobs, the Streamlit app, tests) reads its
settings from here so there is exactly one place to change a hostname.
"""

from __future__ import annotations

import functools
from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

Interval = Literal["1m", "5m", "15m", "1h", "4h", "1d"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        extra="ignore",
    )

    # ---------------------------------------------------------------- pipeline
    symbols: list[str] = Field(
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"],
        description="Binance trading pairs to ingest.",
    )
    interval: Interval = "1d"
    lookback_days: int = 3
    """How far back a scheduled run re-reads. Late/corrected candles are common,
    so we deliberately overlap and rely on idempotent upserts."""

    dq_score_threshold: float = 80.0
    """Threshold (0-100) for the shared DQ engine score computed in
    pipeline.quality.dq_bridge.run_ohlcv_quality_gate, see
    airflow/dags/crypto_market_ingest.py. This mirrors the ETL's own DQ gate
    threshold in the e-commerce project (etl/pipeline.py)."""

    # -------------------------------------------------------------- data lake
    s3_endpoint_url: str = "http://minio:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "market-lake"
    s3_region: str = "us-east-1"

    # -------------------------------------------------------------- warehouse
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_user: str = "warehouse"
    pg_password: str = "warehouse"
    pg_database: str = "market"
    pg_raw_schema: str = "raw"

    # ------------------------------------------------------------------ spark
    spark_master: str = "spark://spark-master:7077"
    spark_driver_memory: str = "1g"
    spark_executor_memory: str = "1g"

    # ------------------------------------------------------------------- http
    binance_base_url: str = "https://api.binance.com"
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    http_timeout_seconds: int = 30
    http_max_retries: int = 5
    binance_requests_per_second: float = 8.0
    """Binance allows 1200 request-weight/minute and a klines call costs 2.
    8 req/s leaves headroom for a parallel backfill running alongside."""
    circuit_breaker_threshold: int = 5

    # ---------------------------------------------------------- observability
    alert_webhook_url: str = ""
    """Slack-compatible incoming webhook. Empty means alerts go to structured
    logs instead, so the callback is safe to leave enabled everywhere."""
    anomaly_z_threshold: float = 3.0
    audit_baseline_runs: int = 30

    @computed_field  # type: ignore[prop-decorator]
    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def pg_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def bronze_path(self, dataset: str, symbol: str, dt: str) -> str:
        """Hive-style partition path, e.g.
        s3a://market-lake/bronze/binance_klines/symbol=BTCUSDT/dt=2026-09-16
        """
        return f"s3a://{self.s3_bucket}/bronze/{dataset}/symbol={symbol}/dt={dt}"

    def silver_path(self, dataset: str) -> str:
        return f"s3a://{self.s3_bucket}/silver/{dataset}"


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor, so importing modules never re-parse the environment."""
    return Settings()
