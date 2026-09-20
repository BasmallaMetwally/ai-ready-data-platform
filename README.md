# AI-Ready Data Platform

[![CI](https://github.com/BasmallaMetwally/ai-ready-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/BasmallaMetwally/ai-ready-data-platform/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)

An end-to-end, portfolio-scale data platform that turns synthetic e-commerce
data and JSON reviews into quality-gated warehouse tables, analytics, and
ML-ready features. It also includes an optional crypto ingestion stack with
Airflow, Spark, dbt, and the same data-quality engine.

**Stack:** Python, Pandas, SQL, SQLite, PostgreSQL, MySQL, FastAPI,
scikit-learn, Airflow, dbt, Spark, Docker, and GitHub Actions.

Arabic overview: [README.ar.md](README.ar.md) · Verification details:
[docs/VERIFICATION.md](docs/VERIFICATION.md) · [DQ component guide](dq/README.md)
· [crypto pipeline guide](ingestion/crypto/README.md)

> The e-commerce data in `data/raw/` is synthetic and exists solely to make
> the project reproducible. The outputs demonstrate engineering workflows;
> they are not business claims.

## In 30 seconds

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 run_all.py --skip-ml

# Browse the API
cd api && uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000/docs` for the interactive API. The command runs
the DQ check, e-commerce ETL, quality gate, SQLite load, and review sentiment
processing. Omit `--skip-ml` to train the four included ML workflows.

## Architecture

```mermaid
flowchart LR
    A[CSV orders, customers, products] --> B[ETL: extract & transform]
    R[JSON customer reviews] --> S[Sentiment features]
    B --> Q{DQ quality gate}
    Q -->|pass| W[(Star-schema warehouse)]
    Q -->|fail| H[Quality history & remediation]
    S --> W
    W --> E[EDA & SQL analytics]
    W --> M[Forecasting · segmentation · anomalies · recommendations]
    W --> F[FastAPI]
    C[Crypto APIs] --> AFS[Airflow · Spark · dbt]
    AFS --> Q
```

## What it demonstrates

- **Quality-gated ETL:** validation, table-level DQ scoring, load blocking,
  remediation, audit history, and trend endpoints.
- **Warehouse modelling:** `dim_customer`, `dim_product`, `dim_date`, and
  `fact_sales` form an e-commerce star schema.
- **Live database targets:** the loader supports `sqlite`, `postgres`, and
  `mysql`; PostgreSQL/MySQL use their native schemas and bulk inserts.
- **Structured and unstructured data:** CSV source tables plus nested JSON
  customer reviews, flattened and enriched with sentiment features.
- **Analytics and ML:** EDA notebook, SQL analysis, forecasting, customer
  segmentation, anomaly detection, and recommendations.
- **Delivery practices:** FastAPI, Docker artifacts, root-level GitHub Actions
  workflow, and an optional Airflow/Spark/dbt crypto pipeline.

## Run the ETL against a database

SQLite is the zero-setup default. PostgreSQL and MySQL require their optional
drivers and a running database. The demo schema is recreated on each load;
use a dedicated development database only.

```bash
# SQLite (default)
python3 run_all.py --skip-ml --target sqlite

# PostgreSQL (DATABASE_URL is the primary configuration path)
pip install -r etl/requirements-db.txt
export DATABASE_URL='postgresql://user:password@localhost:5432/ecommerce_dw'
python3 run_all.py --skip-ml --target postgres

# MySQL 8+
pip install -r etl/requirements-db.txt
export DATABASE_URL='mysql://user:password@localhost:3306/ecommerce_dw'
python3 run_all.py --skip-ml --target mysql
```

The same flags work with `python3 etl/pipeline.py`. PostgreSQL and MySQL
schemas live in [postgres_migration](postgres_migration/) and
[mysql_migration](mysql_migration/).

## Example output

The latest reproducible e-commerce pipeline run produced these DQ-gate scores
(threshold: 80/100):

| Warehouse table | DQ score | Result | Main observation |
| --- | ---: | --- | --- |
| `dim_customer` | 100.0 | pass | No deductions from the configured checks. |
| `dim_product` | 92.7 | pass | IQR outliers: 28 `cost_price`, 27 `unit_price`. |
| `fact_sales` | 92.0 | pass | IQR outliers: 9,432 `total_amount`, 7,703 `unit_price`. |

These are distribution-based outliers in intentionally varied synthetic sales
data—not missing-value penalties or rejected rows. The gate evaluates missing
values, full-row and primary-key duplicates, IQR outliers, configured schema
rules, and date consistency. Scores below the configured threshold block the
load; this run passed at 80/100. See [DQ component guide](dq/README.md) for
the scoring and remediation implementation.

## Validate locally

```bash
# DQ system + ETL loader
python3 -m unittest discover -s dq/tests -v
python3 -m unittest discover -s etl/tests -v

# Crypto pipeline (optional dependencies; run from its project directory)
cd ingestion/crypto/full_stack_optional
pip install -r requirements-dev.txt
PYTHONPATH=src:../../../dq pytest tests/ -v --cov=pipeline --cov-fail-under=85
```

The CI badge is the source of truth for the full GitHub Actions matrix. See
[docs/VERIFICATION.md](docs/VERIFICATION.md) for exact scope, reproducible
commands, and work that requires Docker or live external services.

## Repository map

```text
.
├── dq/                   data-quality checks, scoring, remediation, history, API
├── etl/                  e-commerce extract, transform, validation, and multi-DB load
├── database/             SQLite schema and generated development warehouse
├── ml/                   forecasting, segmentation, anomaly, recommendation workflows
├── api/                  unified FastAPI service
├── notebooks/            executed e-commerce EDA notebook
├── postgres_migration/   PostgreSQL schema and migration utilities
├── mysql_migration/      MySQL 8 schema and migration utilities
└── ingestion/crypto/     crypto ingestion integration and optional full stack
```

## Project status

The e-commerce pipeline and its SQLite target are the quickest reproducible
path. The PostgreSQL/MySQL loader paths are implemented and require local
servers to run. The crypto deployment, Airflow scheduler, dbt execution, and
the live Hugging Face sentiment model depend on Docker and/or network access;
they are deliberately documented as environment-dependent rather than claimed
as completed runs. See [docs/VERIFICATION.md](docs/VERIFICATION.md).
