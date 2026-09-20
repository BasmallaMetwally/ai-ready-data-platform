# Crypto Ingestion — integration notes

## What changed since the first pass (addressing real review feedback)

The first version of this merge kept **two copies** of the crypto
extract/quality code (`ingestion/crypto/pipeline/` and
`full_stack_optional/src/pipeline/`), and `dq_bridge.py` was only ever
called from its own test — not from the actual pipeline. Both were fair
criticisms. Fixed:

1. **One copy of the code.** `ingestion/crypto/pipeline/` is gone. The
   single source of truth is `full_stack_optional/src/pipeline/` — the
   same tree Docker/Airflow mount to `/opt/airflow/src`.
2. **`dq_bridge.py` now lives inside that tree**
   (`full_stack_optional/src/pipeline/quality/dq_bridge.py`), so it ships
   with the code Airflow actually runs instead of sitting off to the side.
3. **It's called for real**, inside
   `full_stack_optional/airflow/dags/crypto_market_ingest.py`'s
   `extract_to_lake` task — right after the existing row-level
   `OHLCV_SUITE` checks, before the batch is written to the bronze lake.
   It computes a 0-100 score with the *same* DQ engine
   (`dq/quality_gate.py`) the e-commerce ETL uses, and records it to the
   same `dq/quality_history.db`, so `GET /quality/history/crypto.BTCUSDT`
   on the unified API shows a real trend once this DAG has run.
4. **`docker-compose.yml`/`Dockerfile` mount `dq/` into the Airflow
   containers** (`../../../dq:/opt/airflow/dq`, added to `PYTHONPATH`) so
   that import actually resolves at runtime — it isn't a dangling
   cross-repo import.
5. A new `dq_score_threshold` setting was added to `pipeline/config.py`
   (default 80.0, mirrors the ETL's own gate threshold) instead of a
   magic number in the DAG.

The row-level `OHLCV_SUITE` checks still raise/skip on hard failures as
before — the new DQ score is a second, complementary signal (a 0-100
score + trend, not yet a hard gate for this DAG). Making a low crypto DQ
score actually block the bronze write (the way the ETL's gate blocks a
warehouse load) is the natural next step if you want it — one `if not
dq_result.passed: raise AirflowSkipException(...)` in `extract_to_lake`.

## What was verified here, and what needs Docker/live network

Docker and live network access to Binance weren't available (see "Not
run end to end" in the main `README.md`). What was verified directly:

- `python3 -m pytest tests/test_dq_bridge.py` — **4/4 passed**, now
  running from the relocated `pipeline/quality/dq_bridge.py`.
- Called `run_ohlcv_quality_gate(rows, symbol=..., threshold=SETTINGS.dq_score_threshold)`
  directly — the exact call the DAG makes — with real
  `pipeline.config.get_settings()` and the real `dq/quality_gate.py`
  module, using the same `PYTHONPATH` layout the container has
  (`src` + `dq`). It resolved and scored correctly.
- `python3 -m py_compile airflow/dags/crypto_market_ingest.py` — syntax
  is valid.
- I could **not** run the DAG through Airflow's own scheduler/DagBag
  import check here — do that with `make up` before trusting it in
  production; that's exactly what the repo's own CI already does on
  every push.

## Everything else (unchanged from before)

See `full_stack_optional/` for the rest of the original project as-is
(dbt models, Spark transform, Streamlit dashboard, docker-compose). Run
it with:

```bash
cd full_stack_optional
cp .env.example .env
make up
make trigger
```

Non-network, non-Spark, non-Postgres tests you can run right now:

```bash
cd full_stack_optional
pip install -r requirements-dev.txt
python3 -m pytest tests/ -q --ignore=tests/test_spark_silver.py \
    --ignore=tests/test_warehouse_integration.py --ignore=tests/test_lake.py
# 71 passed (67 original + 4 dq_bridge tests, one of which always runs
# regardless of whether dq/ is present, to check the fallback path)
```

`.github/workflows/ci.yml` is the original project's own CI, restored
after being dropped from the first merge pass by mistake. Its
`dag-validation` job now also installs `pandas`/`numpy`, since
`pipeline/quality/dq_bridge.py` imports pandas unconditionally (even when
the `dq/` package it wraps isn't present).

**This nested workflow file does not run on its own in this monorepo.**
GitHub Actions only auto-discovers workflows under a repo's *root*
`.github/workflows/`. This file is kept here, unmodified, for the case
where `full_stack_optional/` is extracted back into its own repo (it
becomes that repo's root `.github/workflows/ci.yml` again, unchanged).
For the unified monorepo, the workflow that actually runs is
`../../../.github/workflows/ci.yml` at the repository root — it mirrors
every job here with `working-directory: ingestion/crypto/full_stack_optional`
and `PYTHONPATH=src:../../../dq` so `pipeline.quality.dq_bridge`'s import
of the shared DQ engine resolves during the DAG-validation and test jobs.
The two `pip install` commands and the `PYTHONPATH=src:../../../dq pytest
tests/ ...` command in that root workflow were run locally, exactly as
written, and passed (71/71). The Docker-service-backed steps (Postgres
integration tests, the dbt job, the full Airflow DagBag check) could not
be verified the same way — same limitation as everywhere else in this
repo that needs Docker.

## Two known operational risks with `make up` (not yet fixed — flagged honestly)

These weren't reproducible here (no Docker), so they're documented rather
than fixed blind:

1. **SQLite write contention.** `extract_to_lake` is a dynamically mapped
   task (one instance per symbol, running in parallel). Each instance now
   also calls `run_ohlcv_quality_gate`, which writes a row to
   `dq/quality_history.db` (plain SQLite). Concurrent writers against one
   SQLite file can raise `database is locked`. Quick fixes if you hit it:
   run `bronze_group` with a lower `max_active_tis_per_dag`/pool
   limiting concurrent `extract_to_lake` instances, or point
   `quality_gate.DEFAULT_HISTORY_DB` at a real Postgres table instead of
   SQLite for this DAG specifically.
2. **File permissions on the `dq/` bind mount.** The Airflow image runs
   as the `airflow` user (uid 50000 by default), which may not have
   write access to a `dq/` directory owned by your host user. If
   `quality_history.db` writes fail with a permission error, either
   `chmod`/`chown` the host `dq/` folder for that uid, or mount a
   dedicated writable volume for `quality_history.db` instead of writing
   directly into the bind-mounted source tree.
