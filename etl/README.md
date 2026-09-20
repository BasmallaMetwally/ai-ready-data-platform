# E-commerce ETL Pipeline

An end-to-end pipeline that converts raw e-commerce CSV files into a
star-schema warehouse.

## Flow

`extract → transform → expectations → DQ gate → SQLite / PostgreSQL / MySQL load`

The pipeline retries extraction, validates transformed tables, blocks a load
when a quality score falls below the configured threshold, and logs each step.

## Run it

```bash
cd etl
python3 pipeline.py

# Live database targets (requires a running database)
pip install -r requirements-db.txt
python3 pipeline.py --target postgres --database-url 'postgresql://user:password@localhost:5432/ecommerce_dw'
python3 pipeline.py --target mysql --database-url 'mysql://user:password@localhost:3306/ecommerce_dw'
```

Or run the entire platform from the repository root:

```bash
python3 run_all.py
```

Raw inputs are intentionally synthetic and live in `../data/raw/`. SQLite
output is generated at `../database/warehouse.db` and is not committed.
For the demo, each load recreates the target schema; use a dedicated database,
never production data.
