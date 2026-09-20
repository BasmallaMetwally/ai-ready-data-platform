# E-commerce ETL Pipeline

An end-to-end pipeline that converts raw e-commerce CSV files into a
star-schema warehouse.

## Flow

`extract → transform → expectations → DQ gate → SQLite load`

The pipeline retries extraction, validates transformed tables, blocks a load
when a quality score falls below the configured threshold, and logs each step.

## Run it

```bash
cd etl
python3 pipeline.py
```

Or run the entire platform from the repository root:

```bash
python3 run_all.py
```

Raw inputs are intentionally synthetic and live in `../data/raw/`. The output
warehouse is generated at `../database/warehouse.db` and is not committed.
