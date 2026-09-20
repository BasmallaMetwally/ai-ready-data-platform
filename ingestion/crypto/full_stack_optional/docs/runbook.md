# Runbook

Operational procedures for the on-call engineer. Written in the format a real
data platform team would keep in its repository.

## Service map

| Service | Port | Health check |
|---|---|---|
| `postgres` | 5432 | `pg_isready -U warehouse -d market` |
| `minio` | 9000 / 9001 | `mc ready local` |
| `spark-master` | 7077 / 8081 | Web UI responds |
| `airflow-webserver` | 8080 | `GET /health` |
| `dashboard` | 8501 | Streamlit root responds |

## Common incidents

### `crypto_market_daily.bronze.extract_to_lake` fails with `BinanceError: rate limited`

**Cause.** The extract exceeded Binance's request-weight budget, usually because
a backfill was running concurrently.

**Fix.** The task retries five times with exponential backoff and normally
self-heals. If it does not:

1. Check whether `crypto_backfill` is running: `make ps` then the Airflow UI.
2. Pause the backfill, let the daily DAG complete, then resume.
3. If it persists, lower `max_active_tis_per_dag` in `crypto_backfill.py`.

### Task fails with `DataQualityError`

**This is the system working as designed.** A blocking check caught bad data and
stopped it before the warehouse.

1. Read the task log — the exception names the exact failing checks.
2. Query the offending window directly against the API to confirm whether the
   defect is upstream or in our parsing.
3. If the source data is genuinely wrong, downgrade that specific check to
   `Severity.WARN` *with a code comment explaining why*, and re-run. Never
   disable the suite wholesale.

### `dbt build` fails on `assert_no_missing_trading_days`

**Cause.** A gap in the daily series. Crypto markets never close, so this is
always a real defect.

1. Identify the gap: `make psql`, then
   ```sql
   SELECT trading_pair, min(date_key), max(date_key), count(*)
   FROM analytics.fct_ohlcv_daily GROUP BY 1;
   ```
2. Re-run the backfill for just that range and symbol.
3. Re-run `make dbt-build`.

### Marts are stale (`audit_freshness` fails)

The final DAG task asserts `fct_ohlcv_daily` is at most two days old. If it
fires, the load succeeded but produced no new rows — check the Spark task first,
since a silver job that writes zero partitions still exits successfully.

## Routine operations

### Adding a trading pair

1. Add the Binance symbol to the `crypto_symbols` Airflow Variable.
2. Add its CoinGecko mapping to `SYMBOL_TO_COIN_ID` in `extract/coingecko.py`
   (otherwise it loads prices but gets no name or market cap).
3. Backfill its history: `make backfill FROM=... TO=...`.

### Replaying a bad day

Because every task is idempotent, clearing the run is safe:

```bash
docker compose exec airflow-scheduler \
  airflow tasks clear crypto_market_daily -s 2026-09-01 -e 2026-09-01 --yes
```

### Restoring from bronze

The lake is the source of truth. To rebuild the warehouse from scratch without
touching any API:

```bash
make nuke && make up
docker compose exec airflow-scheduler \
  airflow dags trigger crypto_backfill --conf '{"start_date":"2024-01-01","end_date":"2026-09-01"}'
```

## Escalation

| Symptom | Severity | Action |
|---|---|---|
| One symbol missing for < 24h | Low | Let retries handle it |
| All symbols missing | High | Check Binance status and geo-restrictions |
| dbt test failure on a fact table | High | Do not publish; investigate before the next run |
| Postgres CHECK constraint violation | Critical | A loader bug got past every other gate |
