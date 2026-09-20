# Changelog

## Round 3.3 — after review of round 3.2 (deeper CI verification, coverage-gate bug found)

Went one step further than "clean venv" this time: also installed
`pyspark` and `moto[server]` (the two heavy dependencies `requirements-dev.txt`
pins, skipped in every earlier round to save time/disk) and a real local
PostgreSQL 16 `test_market` database matching the CI Postgres service's
config, then ran the CI `test` job's exact commands — both
`PYTHONPATH=src:../../../dq pytest tests/ -v --cov=pipeline
--cov-report=term-missing --cov-fail-under=85` and the fast-subset
command — for real, not simulated.

- **First run of that exact coverage command failed for real**: 78%
  covered, below the 85% gate — but only because `pyspark`/`moto` were
  still not installed in that first pass. Installing them and re-running
  still left `pipeline/quality/dq_bridge.py` at **0% covered** despite
  its 4 tests passing, dragging total coverage to ~82%. Root cause: an
  earlier version of `tests/test_dq_bridge.py` imported the module
  non-canonically (`sys.path.insert(quality_dir)` + `from dq_bridge
  import ...`) instead of `from pipeline.quality.dq_bridge import ...`
  like every other test in the suite — loading it as a different module
  object than the one `--cov=pipeline` was measuring. Fixed by importing
  it the same way the rest of the codebase does.
- With that fixed, and `pyspark`/`moto[server]` installed alongside a
  real Postgres `test_market` database, the exact CI command now passes:
  **107 tests passed, 89% coverage** (≥85% gate), exit code 0. The
  fast-subset command (`pytest tests/ -m "not integration" -q
  --no-header`) also passes: 71 passed.

This is a stronger claim than round 3.2's "ran the non-Docker parts
locally": the one CI job previously *not* verified this deeply (because
its Postgres-service and heavy-dependency requirements looked like they
needed Docker) turns out to be fully reproducible without Docker too —
just a real Postgres server, `pyspark`, and `moto[server]` installed
locally, all done here. Only the `dbt` job and the full Airflow
scheduler/DagBag-via-GitHub-Actions step remain unverified locally.

## Round 3.2 — after review of round 3.1 (real CI failures reproduced and fixed)

Both fixes below were reproduced from a **clean, isolated environment**
(`python3 -m venv` + only the relevant `requirements*.txt` installed),
not the already-populated environment used for everything else in this
repo's history — closing the gap flagged in the previous review, where
lint and a from-scratch `pip install` hadn't actually been exercised.

- **`ruff check` (6 errors) and `ruff format --check` (2 files) in
  `ingestion/crypto/full_stack_optional`**, both in the files touched
  while building the DQ bridge (`src/pipeline/quality/dq_bridge.py`,
  `tests/test_dq_bridge.py`): unsorted imports, an old-style `Optional[X]`
  string-quoted return annotation instead of `X | None`, `typing.Iterable`
  instead of `collections.abc.Iterable`, and an unused `# noqa: E402`.
  Fixed with `ruff check --fix` + `ruff format`, both files reformatted,
  `ruff check`/`ruff format --check`/`mypy` all now report zero issues,
  and the full test suite (71/71) was re-run afterward to confirm the
  reformatting didn't change behavior.
- **`from fastapi.testclient import TestClient` raised
  `RuntimeError: The starlette.testclient module requires the httpx2
  package to be installed`** in a clean venv with only
  `requirements.txt` installed (it had silently worked everywhere else
  in this repo's history because `httpx` — the older, now-deprecated
  dependency — happened to already be installed from earlier,
  unrelated work). Fixed by adding `httpx2` to `requirements.txt`.
  Reproduced the failure first, then confirmed the fix, in a fresh
  `python3 -m venv`, then re-ran `dq/tests`, `run_all.py --skip-ml`, and
  the API smoke test — the exact commands the root CI workflow runs —
  from that same clean venv, all passing.

## Round 3.1 — after review of round 3

- **Fixed a lying log line**: `etl/process_reviews.py --backend hf`
  logged `backend=hf` even on runs where it had silently fallen back to
  the lexicon backend (missing `torch`/network). `extract_sentiment_features`
  and its HF/lexicon variants now return `(df, backend_used)`, and
  `process_and_load` logs the backend that actually ran, with an explicit
  warning when it differs from what was requested.
- **Fixed a misleading `--compare` output**: it used to print two
  "agreement with star rating" numbers even when the HF backend had
  silently fallen back to lexicon, making it look like a real comparison
  between two methods when both numbers came from the same method. It
  now checks availability first and prints `unavailable` instead of a
  second, identical-by-construction number.
- **Added a root-level `.github/workflows/ci.yml`.** The crypto project's
  own CI (`ingestion/crypto/full_stack_optional/.github/workflows/ci.yml`)
  does not run on its own once this whole thing is one repo — GitHub
  Actions only auto-discovers workflows under the repo root, and a nested
  workflow file is invisible to it. The root workflow mirrors every job
  from the nested one with `working-directory:
  ingestion/crypto/full_stack_optional` and `PYTHONPATH=src:../../../dq`
  so the DQ-bridge import still resolves; the non-Docker parts of it
  (`pip install -r requirements.txt`, `run_all.py --skip-ml`, the API
  smoke test, `PYTHONPATH=src:../../../dq pytest tests/ ...`) were run
  locally exactly as written in the workflow and passed.
- Removed remaining "this build environment" hedging language from
  `README.md`, `ingestion/crypto/README.md`, and `mysql_migration/README.md`
  in favor of stating directly what was tested where (Ubuntu 24.04,
  Python 3.12, MySQL 8.0.46, PostgreSQL 16, all via `apt`, no Docker).

## Round 3 — EDA notebook, MySQL, HF sentiment option

- **`notebooks/01_ecommerce_eda.ipynb`**: EDA on the warehouse
  (`database/warehouse.db`) — data overview, customer age/signup
  distributions, product pricing by category, daily revenue, anomaly
  days, and reviews/sentiment. Executed end to end with outputs saved in
  the file (renders on GitHub without needing to run it), and the last
  section explicitly ties the findings back to the DQ schema checks in
  `etl/pipeline.py`'s `DQ_TABLE_CONFIGS`.
- **`mysql_migration/`**: schema, migration script, docker-compose, and
  four analytical queries (window functions, a CTE, top-N-per-group,
  date/string aggregation), all run against a real local MySQL 8.0.46
  server (installed via `apt-get`, not Docker, in the build
  environment) — not just written. Real, verified differences from
  PostgreSQL are documented in `mysql_migration/README.md`, including a
  genuinely surprising one: `year_month` is a reserved word in MySQL
  (used in `INTERVAL ... YEAR_MONTH`) and fails as an unquoted alias,
  while the identical query works fine in Postgres — confirmed on both
  engines side by side, not assumed from memory.
- **Bug found and fixed in `postgres_migration/migrate_sqlite_to_postgres.py`
  while actually running it for the first time** (it had never been
  executed before this): SQLite stores booleans as 0/1 integers, and
  Postgres's `BOOLEAN` column type rejects a bare integer
  (`psycopg2.errors.DatatypeMismatch`). Hit on `dim_date.is_weekend` and
  `fact_reviews.verified_purchase`; fixed with an explicit `bool()` cast
  for those two columns before insert, then the whole migration was
  re-run successfully against a real Postgres 16 instance. The script's
  docstring, which had claimed `psycopg2` wasn't available in this build
  environment, was also corrected — it installs fine from PyPI.
- **`etl/process_reviews.py`** gained an optional `--backend hf` flag
  (Hugging Face multilingual sentiment model, alongside the existing
  zero-dependency lexicon backend) and a `--compare` flag to measure
  agreement with star ratings on a sample. The HF path gracefully falls
  back to the lexicon backend, with a logged warning, if `transformers`/
  a modeling framework isn't installed or the model can't be downloaded
  — verified directly: `transformers` was installed but `torch` was not
  (and this build environment can't reach huggingface.co either way),
  and running `--backend hf` correctly logged the fallback and completed
  rather than crashing. The model's actual accuracy was not verified —
  see `etl/requirements-nlp.txt`.
- **`run_all.py`** now also runs `etl/process_reviews.py` as step 3 of 5
  (previously a standalone script nothing else orchestrated), with a
  `--skip-reviews` flag; failures there log a warning and don't abort
  the rest of the run, since `fact_reviews` is optional.

## Round 2 — after code review

- Removed the duplicated crypto pipeline copy (`ingestion/crypto/pipeline/`
  was byte-identical to `full_stack_optional/src/pipeline/`; confirmed with
  `diff -rq` before deleting). One copy of the code now.
- Moved `dq_bridge.py` into `full_stack_optional/src/pipeline/quality/` and
  wired it into the real Airflow DAG (`crypto_market_ingest.py`,
  `extract_to_lake` task) — it's no longer only called from its own test.
- Made the `dq/` import in `dq_bridge.py` optional: if `dq/` isn't present
  (e.g. this crypto project deployed as its own standalone repo), DQ
  scoring is skipped with a single warning log instead of crashing the DAG
  import or any task. `tests/test_dq_bridge.py` was updated to match
  (`unittest.skipUnless` for the scoring assertions, plus a new test that
  the missing-package path returns `None` cleanly).
- Added `dq_score_threshold` to `pipeline/config.py` instead of a magic
  number in the DAG.
- `docker-compose.yml` and `airflow/Dockerfile` now mount `../../../dq` to
  `/opt/airflow/dq` and add it to `PYTHONPATH`.
- Restored `.github/workflows/ci.yml` (present in the original
  crypto-market-pipeline repo, dropped by mistake during the round-1
  merge) and updated its `dag-validation` job to also install `pandas`/
  `numpy`, which `pipeline/quality/dq_bridge.py` now imports unconditionally.
- Added `pandas`/`numpy` to `full_stack_optional/requirements.txt` for the
  same reason (previously only pulled in transitively, unverified).
- Restructured the main `README.md`: English became the primary file,
  `README.ar.md` is the Arabic version, and this changelog was split out
  instead of living as a section at the top of the README.
- Documented two known operational risks of the crypto DAG's new step (not
  yet fixed, flagged honestly): SQLite write contention from
  parallel-mapped `extract_to_lake` tasks writing to
  `dq/quality_history.db`, and possible file-permission mismatches on the
  `dq/` bind mount if the Airflow container's user differs from the host's.
  See `ingestion/crypto/README.md`.

## Round 1 — initial merge

- Combined three previously separate projects (DQ system, e-commerce
  ETL/ML/API, crypto ingestion pipeline) into `unified_data_platform/`.
- Added `dq/quality_gate.py`: turned the standalone DQ system into an
  importable gate any pipeline can call.
- Wired that gate into `etl/pipeline.py` as a real step before Load, with
  a configurable per-table threshold; a table below threshold blocks the
  load (`dq/tests/test_quality_gate.py`).
- Built a single unified FastAPI app (`api/main.py`) combining DQ upload/
  remediation endpoints, e-commerce analytics endpoints, and new
  pipeline-trigger / quality-history endpoints.
- Added `run_all.py` as a single entry point for the whole platform.
- Bug fixes found while actually running the merged code:
  - `train_and_save_models.py` never retrained the recommendation model.
  - `get_audit_trail()`'s `numpy.int64` cells broke FastAPI's JSON encoder.
  - `datetime.utcnow()` deprecation in `dq/history.py`.
  - Wrong keyword argument (`primary_key=` vs `exclude_columns=`) calling
    `remediate_outliers()` from the new unified API.
