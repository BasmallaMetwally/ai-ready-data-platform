# MySQL migration

Mirrors `postgres_migration/` — same star schema, same migration-script
structure — targeting MySQL instead. Unlike a lot of "also supports
MySQL" claims, this was run against a **real local MySQL 8.0.46 server**
(installed via `apt-get install mysql-server`, not Docker — Docker
wasn't available either), not just written and assumed to work.

## What was actually verified here

```
$ export MYSQL_URL="mysql://warehouse:warehouse@localhost:3306/ecommerce_dw"
$ python3 migrate_sqlite_to_mysql.py
Migrating SQLite -> MySQL...
Applied MySQL schema.

Migrating tables in FK-safe order:
  dim_customer    -> migrated 2,000 rows
  dim_product     -> migrated 300 rows
  dim_date        -> migrated 900 rows
  fact_sales      -> migrated 86,052 rows
  fact_reviews    -> migrated 12,871 rows

Done.
```

Then all four queries in `analytical_queries.sql` were run against that
same database and returned real results (see that file for the exact
numbers — e.g. query 1's top Electronics product earned 7,809,978.15 in
completed-order revenue).

The same schema and data were **also** migrated to a real local
PostgreSQL 16 instance for a genuine side-by-side (not from memory) — see
"Real differences found" below.

## Setting it up yourself

```bash
# Docker (recommended):
cd mysql_migration
docker compose up -d
export MYSQL_URL="mysql://warehouse:warehouse@localhost:3306/ecommerce_dw"

# or apt (what was actually used to build/test this):
sudo apt-get install mysql-server
sudo service mysql start
mysql -u root -e "
  CREATE DATABASE ecommerce_dw CHARACTER SET utf8mb4;
  CREATE USER 'warehouse'@'localhost' IDENTIFIED BY 'warehouse';
  GRANT ALL PRIVILEGES ON ecommerce_dw.* TO 'warehouse'@'localhost';
  FLUSH PRIVILEGES;
"
export MYSQL_URL="mysql://warehouse:warehouse@localhost:3306/ecommerce_dw"

pip install -r requirements.txt
python3 migrate_sqlite_to_mysql.py
mysql -u warehouse -pwarehouse ecommerce_dw < analytical_queries.sql
```

## Real differences found (by running both engines, not guessing)

| Thing | PostgreSQL | MySQL 8.0 | Found by |
|---|---|---|---|
| Auto-increment PK | `BIGSERIAL` | `BIGINT AUTO_INCREMENT` | writing both schemas |
| Dropping tables out of FK order | `DROP TABLE ... CASCADE` | no `CASCADE` on `DROP TABLE`; wrap drops in `SET FOREIGN_KEY_CHECKS = 0/1` instead | writing both schemas |
| Bulk insert | `psycopg2.extras.execute_values` | no direct equivalent; `cursor.executemany` (slower per-row, batched manually here) | writing the migration scripts |
| `CHECK` constraints | always enforced | only enforced from MySQL 8.0.16+ (silently ignored on 5.x) | documentation, confirmed by testing on 8.0.46 |
| SQLite `0`/`1` → native boolean column | **broke**: `psycopg2.errors.DatatypeMismatch: column "is_weekend" is of type boolean but expression is of type integer` | not an issue — MySQL's `BOOLEAN` *is* `TINYINT(1)`, an int-compatible type | **actually running** `postgres_migration/migrate_sqlite_to_postgres.py` for the first time — it had never been executed before and this bug (plus a second identical one on `fact_reviews.verified_purchase`) was found and fixed while building this MySQL migration |
| Date formatting | `TO_CHAR(date_id, 'YYYY-MM')` | `DATE_FORMAT(date_id, '%Y-%m')` | writing query 4 |
| Distinct string aggregation | `STRING_AGG(DISTINCT col, ', ' ORDER BY col)` | `GROUP_CONCAT(DISTINCT col ORDER BY col SEPARATOR ', ')` | writing query 4 |
| Alias named `year_month` | works fine, unquoted | **real syntax error**: `ERROR 1064 ... near 'year_month'` — `YEAR_MONTH` is a reserved word in MySQL (the interval unit in `INTERVAL '1-2' YEAR_MONTH`) | hit by actually running query 4 with that alias; fixed by renaming to `sale_month` (backtick-quoting `` `year_month` `` also works, demonstrated in the script's comments) |

The boolean bug is the most important one on this list: it means
`postgres_migration/migrate_sqlite_to_postgres.py` had a real bug from
round 1 of this repo's history that nobody had caught because nobody had
run it. It's fixed now (see `postgres_migration/migrate_sqlite_to_postgres.py`'s
`bool_columns` handling) and the fix was verified by re-running the full
migration against a real Postgres instance afterward.

## Making the loader target-agnostic

`etl/load.py` currently only writes to the SQLite warehouse. A
`--target sqlite|postgres|mysql` flag on the ETL loader (so the DQ gate
runs identically regardless of target) is a reasonable next step, not
done here — it would mean parameterizing `etl/load.py`'s SQL dialect
(placeholders, upsert syntax, and the boolean-casting quirk documented
above all differ per target) rather than just pointing the same SQL at a
different connection string.
