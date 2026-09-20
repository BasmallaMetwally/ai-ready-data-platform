# Changelog

## 2.1.0 — verification round

A prior review ran the test suite and confirmed the v2 bug fixes were real, but
also ran the exact CI commands and found three things that hadn't actually been
verified: coverage was 63% against a 70% gate, mypy had 8 errors, and 15 files
failed `ruff format --check`. This round fixed all three for real — and in the
process of writing the tests that closing the coverage gap required, found
**four more genuine bugs** that no amount of re-reading would have surfaced,
because each one only manifests when the code actually runs against a real
S3-compatible store, a real Postgres server, or a real Airflow scheduler.

The pattern holds from the v2 changelog: the hard logic (SCD2, concurrency)
keeps being right, and the failures keep being in exactly the places you can't
verify by reading — a library's runtime behavior, a framework's validation
rules, an environment's actual configuration.

### Fixed — coverage (63% → 89%)

Closed by writing real tests against real systems, not additional mocks:

- **`test_coingecko.py`** (8 tests) — the sibling module to `binance.py` had
  zero tests despite using the same `responses`-based mocking pattern. Writing
  it surfaced a genuine bug (see below).
- **`test_lake.py`** (8 tests) — `pyarrow.fs.S3FileSystem` is a native
  implementation, not boto3-based, so `moto.mock_aws` (which patches botocore)
  does not intercept it. Used `moto`'s standalone server mode instead — a real
  S3-compatible HTTP server, the same relationship pyarrow has with MinIO in
  production — and found two bugs a mocked test would have hidden.
- **`test_warehouse_integration.py`** (11 tests) — installed PostgreSQL 16
  locally and ran the actual `COPY` + `ON CONFLICT` loader against it, including
  the `CHECK` constraints from the DDL. A cursor mock cannot verify a real
  constraint violation; only a real server can.
- **`test_spark_silver.py`** (17 tests) — ran a real local PySpark session
  against `deduplicate`, `enrich` and `quarantine_invalid` directly. These take
  and return DataFrames without touching S3, which is exactly what makes them
  testable in isolation, and exactly why the module is structured that way.
- **`test_logging_conf.py`** (12 tests) — the JSON formatter, idempotent
  `configure()`, and the extra-field allowlist all had zero coverage.

### Fixed — four more bugs, found by making the tests real instead of mocked

- **`ensure_bucket` still didn't create a bucket, even after the FileType.NotFound
  fix.** `pyarrow.fs.S3FileSystem` refuses `create_dir()` on a missing bucket
  unless constructed with `allow_bucket_creation=True`. Every unit-mocked test
  would have passed regardless; only a real S3-compatible server rejects the
  call. Found by the first `test_lake.py` test written.
- **The bronze schema's `nullable=False` was not actually enforced where it
  looked like it was.** `pa.Table.from_pylist()` silently fills a missing dict
  key with null even against a non-nullable field — the constraint is enforced
  by the *parquet writer*, not at table construction. A record missing `close`
  passed straight through table construction and only failed (correctly) inside
  `write_bronze`, with `pyarrow.lib.ArrowInvalid`. The test now asserts the
  specific exception type at the specific call site, not `pytest.raises(Exception)`
  around the wrong function.
- **`CoinGeckoClient._parse` used `reverse[item["id"]]`, an unguarded dict
  lookup.** Writing a test for "the API returns exactly what we asked for"
  required constructing a response with only the requested coins — and doing
  that surfaced that a response containing *any* unrequested item raises an
  unhandled `KeyError`. Changed to `reverse.get(...)` with the unexpected items
  logged and skipped. Real APIs occasionally pad responses; this should degrade,
  not crash.
- **The `crypto_market_ingest` DAG could not be imported by Airflow at all.**
  `sla=dt.timedelta(minutes=30)` was set on `extract_to_lake`, a dynamically
  mapped task (`.expand()`), and Airflow does not support SLAs on mapped tasks
  under any configuration — `DagBag` raised `AirflowException` on every attempt
  to load the file. `ast.parse()` — which the earlier verification round relied
  on for "DAG syntax is sound" — only checks Python grammar; it cannot catch a
  framework-level validation rule. Moved the SLA to `summarise`, the group's
  non-mapped completion task, which is also the semantically correct place for
  it: "all of bronze finished within 30 minutes" is meaningful, "this one of six
  parallel symbols finished within 30 minutes" is not.

### Fixed — everything the original CI commands actually flagged

- **`mypy`: 8 errors → 0.** The most consequential: `on_failure_callback` built
  `severity` via `"critical" if ... else "warning"`, which infers as plain
  `str`, not the `Literal["warning", "critical"]` that `Alert` declares — a
  typo or a third branch added later would have passed mypy and only broken at
  runtime inside `Alert.__init__`. Also fixed a `dict`-unpacking pattern in
  `audited()` where `**base` obscured every field's type from the checker, and
  a `response.json() -> Any` return leaking out of `BinanceClient._get`.
- **`ruff format --check`: 15 files → 0.** Applied the formatter; no logic
  changed, confirmed by the full test suite passing identically before and
  after.
- **`pytest --cov-fail-under=70`: 62.84% → 89.16%.** See above.

### Fixed — dataset and source wiring, found only by actually running dbt and Airflow

Beyond `ast.parse()` and `dbt compile`, this round ran `dbt build` against a
real, seeded PostgreSQL database and imported all four DAGs with a real
`DagBag`. Both had been claimed working on the strength of `dbt compile` (which
never executes a source test) and Python syntax checks (which never invoke
Airflow's validation).

- **Every `raw` source query failed with "cross-database references are not
  implemented."** `_sources.yml` hardcoded `database: market`. The moment the
  connected database had any other name — a CI service container, a locally
  named database, anything but literally "market" — Postgres refused the
  cross-database reference outright. `dbt compile` never caught this because
  compiling never executes a query; only `dbt build`, running source freshness
  and tests for real, does. Removed the `database:` key entirely; dbt now
  resolves the source against whatever database the active target is already
  connected to, which is the only value that's ever actually correct.
- **Both non-file Dataset URIs were not AIP-60 compliant**, a warning in
  Airflow 2.9 and a hard failure in Airflow 3. The postgres URI normalizer
  requires exactly `postgres://<host>/<database>/<schema>/<table>` — four path
  segments. The project's `postgres://market/raw/binance_klines` parsed
  `market` as the *host*, leaving only two path segments. Fixed across all
  three DAGs that declare it, and re-verified — not just that the warning is
  gone, but that `crypto_transform`'s `dataset_triggers` and
  `crypto_market_ingest`'s task `outlets` share a literal, matching URI. The
  CI's DAG-check job was asserting against the old three-segment string and
  using an Airflow API (`timetable.dataset_condition`) that doesn't exist in
  2.9; both are corrected, and the corrected assertion is what actually ran.
- **Two dbt warnings, cleaned up while build output was already being read
  closely**: an unused `seeds:` config block (the project has no seed files),
  and the `tests:` → `data_tests:` rename dbt 1.8 soft-deprecates. Neither was
  a build failure, but a `dbt build` a reviewer runs and sees warnings in
  reads as "not quite finished," which is worth taking seriously.
- **Six numeric columns in the enforced `fct_ohlcv_daily` contract had no
  declared precision/scale**, which dbt flags as a rounding risk on every
  single run. Declared `numeric(20, 10)` for the derived ratio columns and
  confirmed the contract still validates against the actual materialized
  column types — a contract change is exactly the kind of edit that can look
  right and silently stop matching the SQL.

### Fixed — the CI workflow itself, found by running its exact commands, not just the project code

Fixing the project's bugs wasn't enough on its own: `requirements-dev.txt` and
`ci.yml` were still going to reproduce the original 63%-coverage failure on the
very next push, because the CI job had no way to exercise the new integration
tests.

- **`requirements-dev.txt` was missing `pyspark`, `moto[server]`, and a running
  Postgres — the three things `test_lake.py`, `test_spark_silver.py`, and
  `test_warehouse_integration.py` need.** Without them, all three files skip
  via `importorskip`/`skipif`, coverage silently drops back to ~63%, and
  `--cov-fail-under` fails for a reason invisible from the log — it reads as a
  coverage regression when it's actually three missing dependencies. Added all
  three to `requirements-dev.txt` and gave the `test` CI job its own Postgres
  service container (separate from the `dbt` job's) plus `actions/setup-java`,
  since `ubuntu-latest` runners are not guaranteed to ship a JVM and pyspark
  needs one. Raised the coverage gate to 85%, just under the verified 89.16%.
- **The "Enforce data contracts" CI step selected only `fct_ohlcv_daily`,
  not its ancestors**, so it failed immediately on `relation "staging.stg_ohlcv"
  does not exist` — a database error, before the contract was ever checked.
  Changed to `--select +fct_ohlcv_daily`. That still wasn't enough:
  `dim_date` is referenced only through a schema-level `relationships` test,
  not through a `ref()` in the model's SQL, so `+model` selection doesn't
  traverse to it either, and the build failed a second time on
  `relation "analytics.dim_date" does not exist`. Fixed by selecting
  `+fct_ohlcv_daily dim_date` explicitly. Verified by running the exact
  corrected command against a real database: 42/42 pass.
- **The "Verify every model is documented" CI step could never have passed**,
  for any model, regardless of documentation quality. `dbt ls --output json`
  does not include the `description` field unless `--output-keys` explicitly
  requests it — running the original command showed all 9 models reported as
  undocumented, including ones with multi-paragraph descriptions. Added
  `--output-keys name description`.
- **Once the documentation check actually worked, it found a real gap**:
  `int_ohlcv_with_returns`, the intermediate window-function model, had no
  schema.yml entry anywhere in the project — an honest omission, not a
  tooling artifact, and one the broken check had been silently hiding since
  it was added. Added `dbt/models/intermediate/_models.yml`.

Every fix in this section, and every fix above it, is confirmed by running the
literal command from `ci.yml` locally against a real Postgres, a real Spark
session, or a real Airflow `DagBag` — not by re-reading the YAML and reasoning
about what it should do.

### A note on verifying this round

PostgreSQL 16 and PySpark were installed directly in the sandbox (not just
Docker Compose's version) specifically so `warehouse.py` and `spark_silver.py`
could be tested against real systems rather than mocks. Installing
`apache-airflow` afterward, in the *same* Python environment, then broke both:
Airflow's dependencies silently left a stale `pyspark-4.2.0` metadata directory
that shadowed the working 3.5.1 install, and separately upgraded `protobuf`
past what `dbt-common` requires. Both were sandbox cross-contamination from
forcing three heavyweight, mutually incompatible tools into one interpreter —
not a defect in this project. It's also exactly why the real CI workflow
already runs `test`, `dbt`, and `dag-validation` as three separate jobs on
three separate runners: this class of conflict cannot occur there. Each
verification in this round (`dbt build`, the `DagBag` import, `pytest`) is
confirmed independently in transcript, in the isolation that matches how CI
actually runs.

## 2.0.0 — hardening round

### Fixed

Five defects found by re-reading the v1 code. Four of them produced
*plausible-looking wrong numbers* rather than errors, which is the expensive
kind.

- **`dim_asset` left gaps in the SCD2 timeline.** `lead(extracted_at)` was
  computed *before* collapsing unchanged snapshots, so each surviving version
  closed against a snapshot that had been filtered away. Any fact landing in the
  resulting gap matched no dimension row. The window is now derived after the
  collapse, so the validity ranges tile the timeline with no holes.
- **`fct_ohlcv_daily` fabricated orphan foreign keys.** Unmatched facts got a
  synthetic `asset_key` via `generate_surrogate_key`, which pointed at nothing in
  `dim_asset` and made a `relationships` test impossible to write. Replaced with
  a proper Kimball **unknown member** (`asset_key = '-1'`), so referential
  integrity is now actually testable — and tested.
- **The incremental `unique_key` was unsafe.** It keyed on `asset_key`, a type-2
  surrogate that changes whenever the dimension versions. A `delete+insert` would
  therefore miss the pre-version row and silently duplicate that day. Changed to
  the stable `(trading_pair, date_key)`.
- **The Spark job computed `lag()` over a single date partition.** `prior_close`
  and `gap_pct` were null or wrong at every partition boundary. Removed: silver
  now does row-local work only, and all cross-day context is computed in dbt
  where the window sees the full history.
- **`ensure_bucket` never created a bucket.** pyarrow's `get_file_info` returns a
  `NotFound` FileInfo instead of raising, so the `except OSError` branch was
  dead code. Now checks the returned type.
- **Dashboard returns rendered 100× too small.** Streamlit's `"%.2f%%"` is a
  literal percent sign, not a scaling directive, so `0.05` displayed as `0.05%`.

### Added

- **`pipeline/http.py`** — a shared resilient session with a thread-safe
  **token-bucket rate limiter** (throttles *before* the request rather than
  reacting to a 429) and a **circuit breaker** (a hard-down upstream now fails in
  milliseconds instead of consuming the Airflow pool on retry backoff). Both API
  clients use it; CoinGecko no longer imports a private function from the Binance
  module.
- **`pipeline/observability.py`** — the `raw.pipeline_audit` table that v1
  created and never wrote to is now populated by an `audited()` context manager,
  and the `on_failure_callback` the v1 docstring promised actually exists. Alerts
  render Slack blocks, fall back to structured logs without a webhook, and only
  fire on the *final* retry. Every path is failure-tolerant: observability must
  never break the thing it observes.
- **Anomaly detection** — `check_row_count_anomaly` and `check_volume_anomaly`
  compare each run against its own trailing baseline from the audit table. This
  catches the failure mode deterministic rules are blind to: every row
  individually valid, dataset as a whole wrong. Skips rather than fires below
  7 observations, and is WARN severity by design.
- **Dataset-driven scheduling** — the monolithic daily DAG is split into
  `crypto_market_ingest` (API → warehouse) and `crypto_transform` (dbt),
  connected by an Airflow **Dataset** rather than a clock offset. dbt changes no
  longer require re-running extraction, and the two concerns get retry policies
  appropriate to each.
- **Enforced data contracts** on `fct_ohlcv_daily` — all 23 columns declared with
  types and constraints. A rename, drop or type change now fails the build
  instead of a downstream consumer.
- **`dbt snapshot`** for reference-data auditing, and **exposures** declaring the
  dashboard and the freshness audit so `dbt ls --select +exposure:...` shows the
  blast radius before a refactor.
- **`mart_pipeline_health`** — success rate, p95 duration, row-count averages and
  a derived health status per task, as a queryable model rather than something
  you read out of the Airflow UI one task at a time. Surfaced in the dashboard.
- **`dbt test --store-failures`** so an investigation starts with a `SELECT`.
- **pre-commit hooks** and a **sqlfluff** config, plus CI jobs for contract
  enforcement and Dataset wiring (the consumer really is subscribed to the
  producer's outlet).
- **30 new tests**, taking the suite from 17 to 47.

### Changed

- The fixed `time.sleep(0.25)` between pagination calls is gone; the token bucket
  paces requests properly and backfills are faster for it.
- `reload_window_days` is now a dbt var, documented as needing to be ≥ the
  pipeline's `LOOKBACK_DAYS` — otherwise late exchange corrections land in the
  warehouse and never reach the marts.
- `crypto_assets_weekly` publishes to the dataset rather than running dbt inline,
  so exactly one DAG owns the marts and two builds cannot race.
