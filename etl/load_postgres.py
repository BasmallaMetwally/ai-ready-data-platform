"""
load_postgres.py
-------------------
نسخة من etl/load.py بس بتحمّل البيانات في PostgreSQL مباشرة بدل SQLite
(بديل لمرحلة الـ Load في الـ pipeline، مش محتاج تعدي بـ SQLite الأول).

الفرق الوحيد عن load.py: طريقة الاتصال (psycopg2 بدل sqlite3) وطريقة
الإدخال (execute_values للسرعة). باقي الـ pipeline (extract, transform,
validate) بتفضل زي ما هي بالظبط.

الاستخدام:
    export DATABASE_URL="postgresql://ecommerce_user:ecommerce_pass@localhost:5432/ecommerce_dw"
    python3 load_postgres.py
"""

import logging
import os
import sys

logger = logging.getLogger("etl.load_postgres")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "postgres_migration", "schema_postgres.sql")


def get_connection():
    try:
        import psycopg2
    except ImportError:
        raise RuntimeError("لازم تثبت psycopg2 الأول: pip install psycopg2-binary")

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError('حدد DATABASE_URL أولاً، مثال: postgresql://user:pass@localhost:5432/ecommerce_dw')
    return psycopg2.connect(db_url)


def apply_schema(conn):
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        with conn.cursor() as cur:
            cur.execute(f.read())
    conn.commit()
    logger.info("تم تطبيق الـ Postgres schema")


def bulk_insert(conn, df, table_name, exclude_cols=None):
    from psycopg2.extras import execute_values

    exclude_cols = exclude_cols or []
    cols = [c for c in df.columns if c not in exclude_cols]
    values = [tuple(row) for row in df[cols].itertuples(index=False, name=None)]

    placeholders = ", ".join(cols)
    query = f"INSERT INTO {table_name} ({placeholders}) VALUES %s"

    with conn.cursor() as cur:
        execute_values(cur, query, values, page_size=5000)
    conn.commit()
    logger.info("  %-14s -> %6d صف", table_name, len(values))


def load_all(tables: dict):
    conn = get_connection()
    try:
        apply_schema(conn)

        bulk_insert(conn, tables["dim_customer"], "dim_customer")
        bulk_insert(conn, tables["dim_product"], "dim_product")
        bulk_insert(conn, tables["dim_date"], "dim_date")
        bulk_insert(conn, tables["fact_sales"], "fact_sales", exclude_cols=["sale_id"])

        logger.info("تم تحميل كل الجداول بنجاح في PostgreSQL.")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    from extract import extract_all
    from transform import transform_all
    from validate import validate_all

    raw = extract_all()
    transformed = transform_all(raw)
    validate_all(transformed)
    load_all(transformed)
