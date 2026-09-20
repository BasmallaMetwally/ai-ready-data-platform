# Data-platform interview notes

## Why recreate the schema instead of incrementally upsert?

The e-commerce dataset is a reproducible portfolio batch with a bounded,
synthetic source. Recreating the schema makes reruns deterministic, prevents
stale records from masking transformation changes, and keeps the demo simple
to inspect. It is explicitly not a production loading strategy.

For production, use a run watermark and immutable ingestion records, load to
a staging table, validate there, then merge dimensions by natural/business key
and facts by a stable event key. Keep a `pipeline_run_id`, reject late or
duplicate events deterministically, and only promote a successful staging
batch in a transaction. This project’s DQ gate remains immediately before that
promotion step.

## Why did the DQ scores fall below 100?

The current scores are not caused by missing values. `dim_product` is 92.7
because the IQR check flags 28 `cost_price` and 27 `unit_price` values;
`fact_sales` is 92.0 because it flags 9,432 `total_amount` and 7,703
`unit_price` values. The synthetic catalogue deliberately has a wide price
distribution, so this is a useful illustration that generic statistical
outliers are signals for review, not automatically bad data.

For a production deployment, tune thresholds per product category or use
robust, category-aware expectations before treating these records as defects.

## What breaks on schema drift?

The validation layer catches configured type/range violations before loading.
For additive columns, a versioned contract can accept the new field while
preserving the current warehouse schema. For renamed, removed, or incompatible
columns, quarantine the batch, record the failure in quality history, alert the
owner, and evolve the target schema through a reviewed migration—not by
silently coercing data.
