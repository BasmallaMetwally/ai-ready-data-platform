"""Load the e-commerce star schema into SQLite, PostgreSQL, or MySQL.

SQLite remains the default so the project runs without external services.
PostgreSQL and MySQL are live ETL targets when a connection URL is supplied.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("etl.load")

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "database"
DB_PATH = DB_DIR / "warehouse.db"
SCHEMA_PATHS = {
    "sqlite": DB_DIR / "schema.sql",
    "postgres": ROOT_DIR / "postgres_migration" / "schema_postgres.sql",
    "mysql": ROOT_DIR / "mysql_migration" / "schema_mysql.sql",
}
TABLES = ("dim_customer", "dim_product", "dim_date", "fact_sales")


def _require_url(target: str, database_url: str | None) -> str:
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            f"{target} requires --database-url or DATABASE_URL. "
            "See etl/README.md for an example connection URL."
        )
    return url


def _mysql_url_options(url: str) -> dict[str, Any]:
    """Convert a conventional mysql:// URL into mysql-connector options."""
    from urllib.parse import parse_qs, unquote, urlparse

    parsed = urlparse(url.removeprefix("mysql+mysqlconnector://"))
    if parsed.scheme != "mysql" or not parsed.hostname or not parsed.path.strip("/"):
        raise ValueError("MySQL URL must look like mysql://user:password@host:3306/database")
    query = parse_qs(parsed.query)
    options: dict[str, Any] = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": parsed.path.strip("/"),
    }
    if "ssl_disabled" in query:
        options["ssl_disabled"] = query["ssl_disabled"][0].lower() == "true"
    return options


def get_connection(
    target: str = "sqlite", database_url: str | None = None, sqlite_path: str | Path | None = None
) -> Any:
    """Open a DB-API connection for a supported warehouse target."""
    if target not in SCHEMA_PATHS:
        raise ValueError(f"Unsupported target '{target}'. Choose: sqlite, postgres, mysql.")
    if target == "sqlite":
        path = Path(sqlite_path) if sqlite_path else DB_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    url = _require_url(target, database_url)
    if target == "postgres":
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError("Install PostgreSQL support: pip install -r requirements-db.txt") from exc
        return psycopg2.connect(url)
    if target == "mysql":
        try:
            import mysql.connector
        except ImportError as exc:
            raise RuntimeError("Install MySQL support: pip install -r requirements-db.txt") from exc
        return mysql.connector.connect(**_mysql_url_options(url))
    raise AssertionError("validated target was not handled")


def apply_schema(conn: Any, target: str) -> None:
    """Create a fresh warehouse schema for the selected engine."""
    schema = SCHEMA_PATHS[target].read_text(encoding="utf-8")
    if target == "sqlite":
        # The demo schema drops parent dimensions before the fact table.
        # Disable enforcement only while recreating that schema, then turn it
        # back on before inserting data so the loaded warehouse is checked.
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(schema)
        conn.execute("PRAGMA foreign_keys = ON")
    elif target == "mysql":
        cursor = conn.cursor()
        try:
            # mysql-connector's multi-statement API differs across releases.
            # The schema has no stored routines, so executing its individual
            # statements is portable and makes this CI target deterministic.
            statements = [
                statement.strip()
                for statement in "\n".join(
                    line for line in schema.splitlines() if not line.strip().startswith("--")
                ).split(";")
                if statement.strip()
            ]
            for statement in statements:
                cursor.execute(statement)
        finally:
            cursor.close()
    else:
        cursor = conn.cursor()
        try:
            cursor.execute(schema)
        finally:
            cursor.close()
    conn.commit()
    logger.info("Applied %s warehouse schema.", target)


def _normalise_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _rows(df: pd.DataFrame, columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(_normalise_value(value) for value in row) for row in df[columns].itertuples(index=False, name=None)]


def bulk_insert(conn: Any, target: str, df: pd.DataFrame, table_name: str, exclude_cols: tuple[str, ...] = ()) -> None:
    # SQLite stores booleans as 0/1, while PostgreSQL requires actual Python
    # bool values for BOOLEAN columns. Keep the source transformation portable
    # and adapt at the database boundary.
    if target == "postgres" and table_name == "dim_date":
        df = df.copy()
        df["is_weekend"] = df["is_weekend"].astype(bool)
    columns = [column for column in df.columns if column not in exclude_cols]
    rows = _rows(df, columns)
    if not rows:
        return
    cursor = conn.cursor()
    try:
        if target == "postgres":
            from psycopg2.extras import execute_values

            execute_values(cursor, f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s", rows, page_size=5_000)
        else:
            placeholder = "?" if target == "sqlite" else "%s"
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([placeholder] * len(columns))})"
            cursor.executemany(query, rows)
    finally:
        cursor.close()


def load_all(
    tables: dict[str, pd.DataFrame], target: str = "sqlite", database_url: str | None = None,
    sqlite_path: str | Path | None = None,
) -> None:
    """Load transformed tables into ``sqlite``, ``postgres``, or ``mysql``.

    This demo recreates its warehouse schema on each run. Adapt that policy
    before pointing it at a production database.
    """
    conn = get_connection(target, database_url, sqlite_path)
    try:
        apply_schema(conn, target)
        for table_name in TABLES:
            bulk_insert(conn, target, tables[table_name], table_name, ("sale_id",) if table_name == "fact_sales" else ())
        conn.commit()
        cursor = conn.cursor()
        try:
            for table_name in TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                logger.info("  %-14s -> %6d rows", table_name, cursor.fetchone()[0])
        finally:
            cursor.close()
        logger.info("Loaded all tables into %s.", target)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    from extract import extract_all
    from transform import transform_all

    parser = argparse.ArgumentParser(description="Load the ETL star schema into a supported database.")
    parser.add_argument("--target", choices=("sqlite", "postgres", "mysql"), default="sqlite")
    parser.add_argument("--database-url", help="Required for postgres/mysql; defaults to DATABASE_URL.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_all(transform_all(extract_all()), target=args.target, database_url=args.database_url)
