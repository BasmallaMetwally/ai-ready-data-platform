-- ============================================================
-- analytical_queries.sql
-- Four queries, all run against a real local MySQL 8.0.46 instance
-- after migrate_sqlite_to_mysql.py, not just written and assumed to work.
-- Query 4 originally failed — see the note there — and the fix is kept
-- in place rather than silently smoothed over.
-- ============================================================

-- Query 1: window function — top 2 products by revenue in each category
SELECT category, product_name, revenue, category_rank FROM (
  SELECT p.category, p.product_name,
         SUM(f.total_amount) AS revenue,
         RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.total_amount) DESC) AS category_rank
  FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
  WHERE f.status = 'completed'
  GROUP BY p.category, p.product_name
) ranked
WHERE category_rank <= 2
ORDER BY category, category_rank
LIMIT 12;

-- Query 2: CTE + window function — month-over-month revenue growth
WITH monthly AS (
  SELECT d.year, d.month, SUM(f.total_amount) AS revenue
  FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
  WHERE f.status = 'completed'
  GROUP BY d.year, d.month
)
SELECT year, month, revenue,
       LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
             / LAG(revenue) OVER (ORDER BY year, month), 1) AS mom_growth_pct
FROM monthly
ORDER BY year, month;

-- Query 3: top-N per group — top 2 customers by spend, per city
SELECT city, name, total_spent, city_rank FROM (
  SELECT c.city, c.name,
         SUM(f.total_amount) AS total_spent,
         DENSE_RANK() OVER (PARTITION BY c.city ORDER BY SUM(f.total_amount) DESC) AS city_rank
  FROM fact_sales f JOIN dim_customer c ON f.customer_id = c.customer_id
  WHERE f.status = 'completed'
  GROUP BY c.city, c.name
) ranked
WHERE city_rank <= 2
ORDER BY city, city_rank;

-- Query 4: date/string aggregation functions that differ from Postgres.
--
-- IMPORTANT — this query originally used `AS year_month`, which FAILED
-- with a real syntax error on this real MySQL instance:
--   ERROR 1064: ... syntax ... near 'year_month' ...
-- because YEAR_MONTH is a reserved word in MySQL (it's the unit keyword
-- for composite intervals, e.g. INTERVAL '1-2' YEAR_MONTH). The exact
-- same query with the exact same alias runs fine, unquoted, on Postgres
-- 16 — confirmed by actually running both, not assumed. Two ways to fix
-- it in MySQL: quote the identifier with backticks (`` `year_month` ``),
-- or just rename it (done below, since it's simpler and needs no
-- quoting anywhere else the alias is referenced).
SELECT
  DATE_FORMAT(date_id, '%Y-%m') AS sale_month,                          -- Postgres: TO_CHAR(date_id, 'YYYY-MM')
  COUNT(*) AS n_sales,
  GROUP_CONCAT(DISTINCT status ORDER BY status SEPARATOR ', ') AS statuses_seen  -- Postgres: STRING_AGG(DISTINCT status, ', ' ORDER BY status)
FROM fact_sales
GROUP BY sale_month
ORDER BY sale_month;
