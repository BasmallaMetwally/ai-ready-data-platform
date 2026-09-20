"""
migrate_sqlite_to_postgres.py
--------------------------------
ينقل البيانات من الـ SQLite warehouse (اللي بنيناه واختبرناه بالفعل)
لـ PostgreSQL، باستخدام psycopg2.extras.execute_values للـ bulk insert
السريع (أسرع بكتير من إدخال صف صف).

المتطلبات:
    pip install psycopg2-binary
(psycopg2-binary installs fine via pip — confirmed by actually running this
script end to end against a real local PostgreSQL 16 instance, including
finding and fixing the boolean-casting bug above.)

التشغيل:
    export DATABASE_URL="postgresql://ecommerce_user:ecommerce_pass@localhost:5432/ecommerce_dw"
    python3 migrate_sqlite_to_postgres.py
"""

import os
import sqlite3
import sys

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("لازم تثبت psycopg2 الأول: pip install psycopg2-binary")
    sys.exit(1)

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema_postgres.sql")

TABLES_ORDER = ["dim_customer", "dim_product", "dim_date", "fact_sales", "fact_reviews"]


def get_pg_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "حدد متغير البيئة DATABASE_URL أولاً، مثال:\n"
            '  export DATABASE_URL="postgresql://ecommerce_user:ecommerce_pass@localhost:5432/ecommerce_dw"'
        )
    return psycopg2.connect(db_url)


def apply_schema(pg_conn):
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        with pg_conn.cursor() as cur:
            cur.execute(f.read())
    pg_conn.commit()
    print("✅ تم تطبيق الـ Postgres schema")


def migrate_table(sqlite_conn, pg_conn, table_name: str, batch_size: int = 5000):
    cur_sqlite = sqlite_conn.cursor()
    cur_sqlite.execute(f"SELECT * FROM {table_name}")
    columns = [d[0] for d in cur_sqlite.description]

    # sale_id بيتحسب أوتوماتيك (BIGSERIAL) في بوستجريس، فمنعديهوش
    insert_columns = [c for c in columns if c != "sale_id"]
    col_indices = [columns.index(c) for c in insert_columns]

    # BUG FIX (found by actually running this script for the first time):
    # SQLite has no real BOOLEAN type — it stores True/False as the
    # integers 0/1. Postgres's BOOLEAN column rejects a bare integer
    # (psycopg2.errors.DatatypeMismatch: "column is of type boolean but
    # expression is of type integer"), so any column that's boolean in
    # schema_postgres.sql needs an explicit int -> bool cast here.
    bool_columns = {
        "dim_date": {"is_weekend"},
        "fact_reviews": {"verified_purchase"},
    }.get(table_name, set())
    bool_indices = {insert_columns.index(c) for c in bool_columns if c in insert_columns}

    placeholders = ", ".join(insert_columns)
    query = f"INSERT INTO {table_name} ({placeholders}) VALUES %s"

    total = 0
    with pg_conn.cursor() as cur_pg:
        while True:
            rows = cur_sqlite.fetchmany(batch_size)
            if not rows:
                break
            values = [
                tuple(
                    bool(row[i]) if pos in bool_indices else row[i]
                    for pos, i in enumerate(col_indices)
                )
                for row in rows
            ]
            execute_values(cur_pg, query, values)
            total += len(rows)

    pg_conn.commit()
    print(f"  {table_name:<15} -> تم نقل {total:,} صف")


def main():
    print("بدء الترحيل من SQLite إلى PostgreSQL...")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    pg_conn = get_pg_connection()

    try:
        apply_schema(pg_conn)

        print("\nنقل البيانات جدول جدول (بالترتيب الصحيح لاحترام الـ Foreign Keys):")
        for table in TABLES_ORDER:
            # لو الجدول مش موجود في SQLite (زي fact_reviews لو المستخدم معملش process_reviews.py)
            exists = sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if exists:
                migrate_table(sqlite_conn, pg_conn, table)
            else:
                print(f"  {table:<15} -> تخطي (غير موجود في SQLite)")

        print("\n✅ تم الترحيل بنجاح!")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
