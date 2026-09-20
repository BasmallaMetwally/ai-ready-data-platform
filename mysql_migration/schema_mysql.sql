-- ============================================================
-- schema_mysql.sql
-- Same star schema as schema_postgres.sql, in MySQL 8.0 syntax.
-- Differences from the Postgres version, found by actually creating
-- both and comparing (not just translated blind):
--
--   - AUTO_INCREMENT instead of SERIAL/BIGSERIAL.
--   - No CASCADE on DROP TABLE (MySQL doesn't support DROP ... CASCADE;
--     drop order matters instead — children before parents, or disable
--     FK checks around the drop, done below).
--   - CHECK constraints ARE enforced in MySQL 8.0.16+, unlike MySQL 5.x
--     where they were silently ignored — this schema assumes 8.0+
--     (docker-compose.yml below pins `mysql:8.0`).
--   - NUMERIC(p,s) works the same as Postgres for money columns.
--   - Identifiers here don't need quoting since none collide with
--     reserved words, but MySQL's identifier quote character is a
--     backtick (`) rather than Postgres's double quote (") if you ever
--     need one — e.g. a column literally named `option` would be
--     `` `option` `` here vs `"option"` in Postgres.
--   - BOOLEAN is a synonym for TINYINT(1) in MySQL, not a real boolean
--     type. It round-trips fine with Python bool or 0/1 either way.
-- ============================================================

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS fact_reviews;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;

SET FOREIGN_KEY_CHECKS = 1;

-- ---------- Dimension Tables ----------

CREATE TABLE dim_customer (
    customer_id     INTEGER PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    signup_date     DATE NOT NULL,
    age             SMALLINT CHECK (age BETWEEN 0 AND 120),
    age_group       VARCHAR(10)
) ENGINE=InnoDB;

CREATE TABLE dim_product (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    cost_price      NUMERIC(12, 2) NOT NULL CHECK (cost_price >= 0),
    margin_pct      NUMERIC(6, 4)
) ENGINE=InnoDB;

CREATE TABLE dim_date (
    date_id         DATE PRIMARY KEY,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day             SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    weekday_name    VARCHAR(15) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    quarter         SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4)
) ENGINE=InnoDB;

-- ---------- Fact Table ----------

CREATE TABLE fact_sales (
    sale_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id        INTEGER NOT NULL,
    item_id         INTEGER NOT NULL,
    customer_id     INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    date_id         DATE    NOT NULL,
    quantity        SMALLINT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
    status          VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'cancelled', 'returned', 'unknown')),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_sales_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id),
    CONSTRAINT fk_sales_date     FOREIGN KEY (date_id)     REFERENCES dim_date(date_id)
) ENGINE=InnoDB;

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product  ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date     ON fact_sales(date_id);

-- ---------- Reviews (optional; only populated if etl/process_reviews.py ran) ----------

CREATE TABLE fact_reviews (
    review_id           BIGINT PRIMARY KEY,
    product_id          INTEGER NOT NULL,
    customer_id         INTEGER NOT NULL,
    review_date         DATE,
    rating              SMALLINT CHECK (rating BETWEEN 1 AND 5),
    device              VARCHAR(20),
    verified_purchase   TINYINT(1),
    helpful_votes       INTEGER,
    images_attached     TINYINT(1),
    review_text_clean   TEXT,
    word_count          INTEGER,
    sentiment_score     INTEGER,
    sentiment_label     VARCHAR(10),
    CONSTRAINT fk_reviews_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_reviews_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id)
) ENGINE=InnoDB;
