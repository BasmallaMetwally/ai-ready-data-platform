# Verification and environment notes

This file keeps execution evidence and environment constraints out of the
project landing page. Re-run the commands below when reporting test counts in
a CV or portfolio: the GitHub Actions badge is the authoritative result for
the complete CI matrix.

## Reproducible local checks

```bash
pip install -r requirements.txt
python3 -m unittest discover -s dq/tests -v
python3 -m unittest discover -s etl/tests -v
python3 run_all.py --skip-ml
```

The SQLite path requires no services. PostgreSQL and MySQL loading requires
the optional drivers in `etl/requirements-db.txt` and a database specified
with `--database-url` (or `DATABASE_URL`). Each demo load applies its schema,
which drops and recreates the target tables; never aim it at production data.

## CI matrix

The root workflow at `.github/workflows/ci.yml` contains four jobs:

- DQ, ETL, and API smoke checks
- Crypto linting plus unit/integration tests against PostgreSQL
- Airflow DAG import validation
- dbt dependency, compile, and test checks against PostgreSQL

The nested workflow under `ingestion/crypto/full_stack_optional/` is retained
only for extracting that directory into a standalone repository; GitHub does
not discover it from this monorepo root.

## External-service checks

These tasks are intentionally not represented as a completed local run unless
the required environment is available:

- Airflow scheduler and the Docker-compose crypto deployment require Docker.
- Live Binance/CoinGecko ingestion requires network access and API availability.
- `python3 etl/process_reviews.py --compare` needs `transformers`, a backend
  framework such as PyTorch, and model access from Hugging Face. Its agreement
  metric uses the review star rating as a proxy, not human-labelled sentiment
  ground truth.

## Reporting guidance

Do not hard-code an aggregate test count in the README or CV. Report the
current green GitHub Actions result with its run URL/date, because the test
matrix and coverage can change. Do not list Hugging Face Transformers as
validated experience until the live comparison has completed successfully.
