-- Warehouse bootstrap. Idempotent: safe to run on every container start.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA raw       IS 'Landing zone written by Airflow loaders. Never edited by hand.';
COMMENT ON SCHEMA staging   IS 'dbt views and ephemeral models.';
COMMENT ON SCHEMA analytics IS 'Star schema consumed by BI. The only schema analysts should query.';

-- ---------------------------------------------------------------- raw tables
CREATE TABLE IF NOT EXISTS raw.binance_klines (
    symbol                   TEXT           NOT NULL,
    interval                 TEXT           NOT NULL,
    open_time                TIMESTAMPTZ    NOT NULL,
    close_time               TIMESTAMPTZ    NOT NULL,
    open                     NUMERIC(38, 12) NOT NULL,
    high                     NUMERIC(38, 12) NOT NULL,
    low                      NUMERIC(38, 12) NOT NULL,
    close                    NUMERIC(38, 12) NOT NULL,
    volume                   NUMERIC(38, 12) NOT NULL,
    quote_volume             NUMERIC(38, 12) NOT NULL,
    trade_count              BIGINT         NOT NULL,
    taker_buy_base_volume    NUMERIC(38, 12),
    taker_buy_quote_volume   NUMERIC(38, 12),
    loaded_at                TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT pk_binance_klines PRIMARY KEY (symbol, interval, open_time),

    -- Enforce the OHLC invariants at the database boundary, not just in dbt.
    -- A constraint cannot be bypassed by a rogue backfill script.
    CONSTRAINT chk_klines_high  CHECK (high >= low AND high >= open AND high >= close),
    CONSTRAINT chk_klines_low   CHECK (low  <= open AND low  <= close),
    CONSTRAINT chk_klines_price CHECK (open > 0 AND close > 0),
    CONSTRAINT chk_klines_vol   CHECK (volume >= 0 AND quote_volume >= 0),
    CONSTRAINT chk_klines_time  CHECK (close_time > open_time)
);

CREATE INDEX IF NOT EXISTS ix_klines_open_time ON raw.binance_klines (open_time DESC);
CREATE INDEX IF NOT EXISTS ix_klines_symbol_time ON raw.binance_klines (symbol, open_time DESC);

CREATE TABLE IF NOT EXISTS raw.coingecko_assets (
    coin_id              TEXT           NOT NULL,
    symbol               TEXT           NOT NULL,
    trading_pair         TEXT           NOT NULL,
    name                 TEXT           NOT NULL,
    market_cap_usd       NUMERIC(38, 2),
    market_cap_rank      INTEGER,
    circulating_supply   NUMERIC(38, 6),
    max_supply           NUMERIC(38, 6),
    all_time_high_usd    NUMERIC(38, 12),
    all_time_high_date   DATE,
    extracted_at         TIMESTAMPTZ    NOT NULL,
    loaded_at            TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT pk_coingecko_assets PRIMARY KEY (coin_id, extracted_at),
    CONSTRAINT chk_assets_supply CHECK (
        max_supply IS NULL OR circulating_supply IS NULL OR circulating_supply <= max_supply * 1.01
    )
);

-- ----------------------------------------------------------- run audit table
-- Every pipeline run writes one row here. Gives you a queryable history of
-- volumes and durations without digging through Airflow's metadata database.
CREATE TABLE IF NOT EXISTS raw.pipeline_audit (
    run_id        TEXT        NOT NULL,
    dag_id        TEXT        NOT NULL,
    task_id       TEXT        NOT NULL,
    logical_date  DATE        NOT NULL,
    row_count     BIGINT,
    status        TEXT        NOT NULL CHECK (status IN ('success', 'failed', 'skipped')),
    duration_sec  NUMERIC(10, 2),
    message       TEXT,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_pipeline_audit PRIMARY KEY (run_id, task_id)
);

CREATE INDEX IF NOT EXISTS ix_audit_logical_date ON raw.pipeline_audit (logical_date DESC);

-- ------------------------------------------------------------------- grants
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE ROLE analyst NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA analytics TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO analyst;
