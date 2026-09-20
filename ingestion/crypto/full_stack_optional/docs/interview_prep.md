# Interview preparation

The questions this project invites, and answers grounded in the actual code.
Every answer points at a file you can open — that is what separates "I built a
pipeline" from "I made these decisions".

---

## Architecture

**"Walk me through the pipeline."**

Binance REST → immutable parquet in S3 (bronze) → Spark cleans and deduplicates
(silver) → PostgreSQL → dbt builds a tested star schema → Streamlit. Airflow
orchestrates, split into an ingestion DAG and a transformation DAG connected by
a Dataset.

The medallion split earns its keep at the bronze layer specifically: when a
business rule changes, I replay from parquet instead of re-pulling years of
history from an API that rate-limits me.

**"Why Spark for a few thousand rows a day? Isn't that over-engineering?"**

Alone, yes. The justification is `crypto_backfill`: the same job runs unchanged
over a multi-year minute-level backfill — tens of millions of candles — where a
pandas implementation runs out of memory. I use Spark where the volume is, not
everywhere. Note that the warehouse load is *not* Spark JDBC; it is a `COPY`
loader, because at that volume COPY is faster and keeps transactional upsert
semantics.

**"Why split into two DAGs?"**

In v1 it was one. Three problems: testing a dbt change meant re-running
extraction; the backfill DAG couldn't reuse the transform step; and an API call
and a SQL build were sharing one `default_args`, even though an API call should
retry five times and a failing SQL model will fail identically on retry.

The Dataset (`postgres://market/raw/binance_klines`) keeps them correctly
ordered without a sleep-and-hope schedule offset. `crypto_transform` starts when
ingestion publishes, whether that's the 01:00 run, a manual trigger, or the tail
of a backfill.

---

## Correctness

**"How do you guarantee you can safely re-run anything?"**

Three mechanisms, in `load/lake.py`, `load/warehouse.py` and
`fct_ohlcv_daily.sql`: parquet writes overwrite by partition, Postgres loads are
`COPY` into a temp table then `INSERT ... ON CONFLICT DO UPDATE`, and the fact
table uses `delete+insert` over a rolling window. Every task converges to the
same state on re-run, which is what makes `catchup=True` and casual retries safe.

**"Tell me about a bug you found in your own code."** — *the question this
project is built to answer*

The SCD2 in `dim_asset`. I computed `lead(extracted_at)` to close each validity
window, then filtered out snapshots where nothing changed. Wrong order: each
surviving version closed against a snapshot that had been filtered away, leaving
gaps in the timeline. Facts landing in a gap matched no dimension row.

It produced no error. Some rows just quietly lost their asset attributes. I
found it re-reading the CTE order, and the fix was to derive the window *after*
the collapse. That class of bug — plausible wrong numbers, no exception — is why
I added the unknown member and a real `relationships` test: so the next
occurrence fails the build instead of hiding.

**"Why is `fct_asset_metrics_daily` a full rebuild when the other fact is
incremental?"**

Its windows look back 200 days. An incremental build computes wrong values at
every slice boundary, because the rows needed for the window aren't in scope.
The numbers would still *look* like moving averages. Some models genuinely
cannot be incremental, and recognising which is the skill.

**"What's the unknown member for?"**

Kimball's answer to a fact arriving for an entity the dimension has never seen.
Without it you either carry an orphan key that breaks referential integrity, or
drop the row and your measures silently stop adding up. With it, the star schema
stays intact, `has_unknown_asset` flags the rows, and the freshness audit alerts
on them.

---

## Data quality

**"How do you stop bad data reaching a dashboard?"**

Four independent layers — see the diagram in the README. Row-level checks before
the load (`quality/checks.py`), Spark quarantine for rows failing OHLC
invariants, `CHECK` constraints in the DDL that no script can bypass, then 40+
dbt tests.

**"Deterministic checks catch corrupt rows. What catches a dataset that's wrong
but not corrupt?"**

That's exactly the gap `check_row_count_anomaly` fills. It compares each run
against its own trailing baseline from the audit table — if an upstream starts
returning half its usual volume, every individual row is valid and every rule
passes. Three design choices: it skips below 7 observations (a z-score over a
handful of points is noise), it's WARN not ERROR (real market events do move
volume several sigma), and it degrades to "skipped" if the audit table is
unreachable.

**"Why WARN and ERROR severities rather than just failing?"**

Because a suite that fails on everything gets disabled. `taker_buy_volume`
slightly exceeding total volume is a rounding artefact worth logging; a negative
price is not. The suite also **fails closed** — a predicate that throws counts
as a failure, never as a pass. There's a test for that
(`test_missing_field_fails_closed`).

---

## Operations

**"How do you know the pipeline is healthy?"**

`mart_pipeline_health` — success rate, p95 duration, average row count and a
derived status per task, as a dbt model. Airflow's metadata database answers
"did the task run?"; this answers "did it load what it should have?", which is a
different question and the one that matters. Telemetry is data, so it lives in
the warehouse where I can join it to the facts.

**"What happens at 3am when Binance goes down?"**

The circuit breaker in `http.py` opens after 5 consecutive failures and fails
subsequent calls in microseconds instead of burning 30 minutes of exponential
backoff per task across a mapped pool. The failure callback alerts only on the
final retry — alerting on every attempt is how teams learn to ignore alerts.
`docs/runbook.md` has the triage steps.

**"How do you add a new asset?"**

Change the `crypto_symbols` Airflow Variable — dynamic task mapping generates
the extract task at runtime, so no deploy. Add the CoinGecko mapping so it gets
a name and market cap, then backfill. Documented in the runbook.

---

## Questions to be honest about

Interviewers respect a known limitation more than an over-claim. These are real:

- **Single exchange.** Prices are Binance-only; thin pairs diverge from the
  global composite. A real trading desk would need multi-venue consolidation.
- **USDT is treated as USD.** It has historically depegged by a few percent
  under stress. Documented in the data dictionary, not silently assumed.
- **Batch, not streaming.** Daily bars. Minute-level or tick data would need a
  Kafka + Structured Streaming path alongside the batch layer.
- **`dim_asset` only versions on market-cap rank.** Other attributes are
  overwritten; the `dbt snapshot` exists as the audit trail for those, which is
  a deliberate trade-off rather than an oversight.
- **Secrets are in `.env`.** Fine for a local stack; production needs a real
  secrets manager and an Airflow secrets backend.

---

## If you get 30 seconds

> "It's a medallion-architecture ELT pipeline for crypto market data — Airflow,
> Spark, dbt on PostgreSQL, all in Docker Compose. What I'd actually want to
> talk about is the verification process: I ran the actual CI commands instead
> of trusting them, found real gaps — a coverage shortfall, type errors, a DAG
> that Airflow couldn't even import — and while writing real tests to close
> those gaps, found further bugs that only show up when the code runs against
> a real database or a real Spark session, not a mock. The changelog documents
> each one and why re-reading the code would never have caught it."

## A note before you use any of this

Everything above is accurate to what was actually run and checked — see
[Verification scope](../README.md#verification-scope) in the README for
exactly where that verification stops. But a CHANGELOG documenting a bug is
not the same thing as you being able to explain it. If an interviewer asks a
follow-up on any answer here — "why does closing the collapse-then-lag order
matter", "what would break if you used `merge` instead of `delete+insert`" —
the file won't answer for you. Read the actual code each answer points to,
not just this summary of it, before relying on it in a live conversation.
