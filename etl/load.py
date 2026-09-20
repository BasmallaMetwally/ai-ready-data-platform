"""
load.py
-------
طبقة التحميل (Load) في الـ ETL Pipeline.
مسؤولة عن كتابة جداول الـ Star Schema في قاعدة بيانات SQLite
(هنا استخدمنا SQLite عشان تشتغل من غير سيرفر خارجي، لكن الكود
شغال بنفس المنطق مع PostgreSQL عن طريق تغيير الـ connection string فقط).
"""

import os
import sqlite3
import logging

logger = logging.getLogger("etl.load")

DB_DIR = os.path.join(os.path.dirname(__file__), "..", "database")
DB_PATH = os.path.join(DB_DIR, "warehouse.db")
SCHEMA_PATH = os.path.join(DB_DIR, "schema.sql")


def get_connection() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def apply_schema(conn: sqlite3.Connection):
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    logger.info("تم تطبيق الـ schema على قاعدة البيانات: %s", DB_PATH)


def load_all(tables: dict):
    conn = get_connection()
    try:
        apply_schema(conn)

        tables["dim_customer"].to_sql("dim_customer", conn, if_exists="append", index=False)
        tables["dim_product"].to_sql("dim_product", conn, if_exists="append", index=False)
        tables["dim_date"].to_sql("dim_date", conn, if_exists="append", index=False)

        fact = tables["fact_sales"]  # sale_id is autoincrement, item_id kept as data column
        fact.to_sql("fact_sales", conn, if_exists="append", index=False)

        conn.commit()
        logger.info("تم تحميل كل الجداول بنجاح في قاعدة البيانات.")

        cur = conn.cursor()
        for t in ["dim_customer", "dim_product", "dim_date", "fact_sales"]:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            logger.info("  %-14s -> %6d صف", t, cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from extract import extract_all
    from transform import transform_all

    raw = extract_all()
    transformed = transform_all(raw)
    load_all(transformed)
