"""
sql_analysis.py
----------------
مجموعة استعلامات SQL متقدمة (Window Functions, CTEs, Joins) على الـ Data
Warehouse، بتجاوب على أسئلة تحليلية حقيقية بتتسأل في أي شركة e-commerce.
"""

import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")


def run_query(conn, title, query):
    print(f"\n{'='*70}\n{title}\n{'='*70}")
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    return df


QUERIES = {

"أعلى 10 عملاء إنفاقاً (فقط الطلبات المكتملة)": """
    SELECT c.customer_id, c.name, c.city,
           ROUND(SUM(f.total_amount), 2) AS total_spent,
           COUNT(DISTINCT f.order_id)     AS n_orders
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    WHERE f.status = 'completed'
    GROUP BY c.customer_id, c.name, c.city
    ORDER BY total_spent DESC
    LIMIT 10;
""",

"نمو المبيعات الشهري مع نسبة التغيير (Window Function - LAG)": """
    WITH monthly_sales AS (
        SELECT strftime('%Y-%m', d.date_id) AS month,
               ROUND(SUM(f.total_amount), 2) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE f.status = 'completed'
        GROUP BY month
    )
    SELECT month, revenue,
           ROUND(revenue - LAG(revenue) OVER (ORDER BY month), 2) AS change_abs,
           ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
                 / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2) AS change_pct
    FROM monthly_sales
    ORDER BY month;
""",

"أفضل منتج مبيعاً في كل فئة (Window Function - RANK)": """
    WITH product_sales AS (
        SELECT p.category, p.product_name,
               SUM(f.total_amount) AS revenue,
               RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.total_amount) DESC) AS rnk
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        WHERE f.status = 'completed'
        GROUP BY p.category, p.product_name
    )
    SELECT category, product_name, ROUND(revenue, 2) AS revenue
    FROM product_sales
    WHERE rnk = 1
    ORDER BY revenue DESC;
""",

"معدل الإلغاء والإرجاع لكل فئة منتج": """
    SELECT p.category,
           COUNT(*) AS total_items,
           SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
           SUM(CASE WHEN f.status = 'returned'  THEN 1 ELSE 0 END) AS returned,
           ROUND(100.0 * SUM(CASE WHEN f.status IN ('cancelled','returned') THEN 1 ELSE 0 END)
                 / COUNT(*), 2) AS problem_rate_pct
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.category
    ORDER BY problem_rate_pct DESC;
""",

"متوسط قيمة الطلب حسب المدينة": """
    WITH order_totals AS (
        SELECT f.order_id, c.city, SUM(f.total_amount) AS order_value
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_id = c.customer_id
        WHERE f.status = 'completed'
        GROUP BY f.order_id, c.city
    )
    SELECT city,
           COUNT(*) AS n_orders,
           ROUND(AVG(order_value), 2) AS avg_order_value
    FROM order_totals
    GROUP BY city
    ORDER BY avg_order_value DESC;
""",

}


def main():
    conn = sqlite3.connect(DB_PATH)
    results = {}
    for title, q in QUERIES.items():
        results[title] = run_query(conn, title, q)
    conn.close()
    return results


if __name__ == "__main__":
    main()
