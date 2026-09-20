-- ============================================================
-- schema.sql
-- Star Schema لمستودع بيانات (Data Warehouse) لمتجر إلكتروني
-- متوافق مع SQLite / PostgreSQL (مع فروق بسيطة في الأنواع)
-- ============================================================

-- ---------- Dimension Tables ----------

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_id     INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    city            TEXT NOT NULL,
    signup_date     DATE NOT NULL,
    age             INTEGER,
    age_group       TEXT            -- محسوبة: '18-25','26-35','36-45','46-55','56+'
);

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id      INTEGER PRIMARY KEY,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    unit_price      REAL NOT NULL,
    cost_price      REAL NOT NULL,
    margin_pct      REAL            -- محسوبة: (unit_price - cost_price) / unit_price
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id         DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    weekday_name    TEXT NOT NULL,
    is_weekend      INTEGER NOT NULL,   -- 0/1
    quarter         INTEGER NOT NULL
);

-- ---------- Fact Table ----------

DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    sale_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        INTEGER NOT NULL,
    item_id         INTEGER NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    product_id      INTEGER NOT NULL REFERENCES dim_product(product_id),
    date_id         DATE    NOT NULL REFERENCES dim_date(date_id),
    quantity        INTEGER NOT NULL,
    unit_price      REAL NOT NULL,
    total_amount    REAL NOT NULL,      -- quantity * unit_price
    status          TEXT NOT NULL       -- completed / cancelled / returned
);

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product  ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date     ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_status   ON fact_sales(status);
