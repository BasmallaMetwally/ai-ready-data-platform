-- ============================================================
-- schema_postgres.sql
-- نفس الـ Star Schema بتاع SQLite، بس بصيغة PostgreSQL الصحيحة:
--   - SERIAL بدل AUTOINCREMENT
--   - أنواع بيانات دقيقة أكتر (NUMERIC للمبالغ المالية بدل REAL)
--   - CHECK constraints للتحقق من صحة البيانات على مستوى القاعدة نفسها
--   - Foreign Keys حقيقية (SQLite بيدعمها بس مش بيفعّلها افتراضياً)
-- ============================================================

-- ---------- Dimension Tables ----------

DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
    customer_id     INTEGER PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    signup_date     DATE NOT NULL,
    age             SMALLINT CHECK (age BETWEEN 0 AND 120),
    age_group       VARCHAR(10)
);

DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    cost_price      NUMERIC(12, 2) NOT NULL CHECK (cost_price >= 0),
    margin_pct      NUMERIC(6, 4)
);

DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_id         DATE PRIMARY KEY,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day             SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    weekday_name    VARCHAR(15) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    quarter         SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4)
);

-- ---------- Fact Table ----------

DROP TABLE IF EXISTS fact_sales CASCADE;
CREATE TABLE fact_sales (
    sale_id         BIGSERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL,
    item_id         INTEGER NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    product_id      INTEGER NOT NULL REFERENCES dim_product(product_id),
    date_id         DATE    NOT NULL REFERENCES dim_date(date_id),
    quantity        SMALLINT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount    NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
    status          VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'cancelled', 'returned', 'unknown'))
);

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product  ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date     ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_status   ON fact_sales(status);

-- ---------- Reviews Fact Table (بيانات غير منظمة بعد المعالجة) ----------

DROP TABLE IF EXISTS fact_reviews CASCADE;
CREATE TABLE fact_reviews (
    review_id           INTEGER PRIMARY KEY,
    product_id          INTEGER NOT NULL REFERENCES dim_product(product_id),
    customer_id         INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    review_date         DATE NOT NULL,
    rating              SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    device              VARCHAR(30),
    verified_purchase   BOOLEAN,
    helpful_votes       INTEGER DEFAULT 0,
    images_attached     SMALLINT DEFAULT 0,
    review_text_clean   TEXT,
    word_count          INTEGER,
    sentiment_score      INTEGER,
    sentiment_label      VARCHAR(10)
);

CREATE INDEX idx_fact_reviews_product ON fact_reviews(product_id);
CREATE INDEX idx_fact_reviews_sentiment ON fact_reviews(sentiment_label);
