"""
transform.py
------------
طبقة التحويل (Transform) في الـ ETL Pipeline.
مسؤولة عن:
  1) تنظيف البيانات (missing values, duplicates, outliers)
  2) بناء جداول الـ Star Schema (dim_customer, dim_product, dim_date, fact_sales)
"""

import numpy as np
import pandas as pd
import logging

logger = logging.getLogger("etl.transform")


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()
    before = len(df)

    # إزالة التكرار
    df = df.drop_duplicates(subset="customer_id")

    # تعويض القيم الناقصة في العمر بالمتوسط
    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age).round().astype(int)

    # تصنيف الفئة العمرية
    bins = [0, 25, 35, 45, 55, 200]
    labels = ["18-25", "26-35", "36-45", "46-55", "56+"]
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels)

    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date

    logger.info("clean_customers: %d -> %d صف (بعد إزالة %d تكرار)",
                before, len(df), before - len(df))
    return df[["customer_id", "name", "city", "signup_date", "age", "age_group"]]


def clean_products(products: pd.DataFrame) -> pd.DataFrame:
    df = products.copy()
    df = df.drop_duplicates(subset="product_id")
    df["margin_pct"] = ((df["unit_price"] - df["cost_price"]) / df["unit_price"]).round(4)
    return df


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()
    before = len(df)

    # القيم الناقصة في status نعتبرها "unknown" بدل ما نمسحها (نحافظ على البيانات)
    df["status"] = df["status"].fillna("unknown")
    df["order_date"] = pd.to_datetime(df["order_date"])

    logger.info("clean_orders: %d صف (status الناقصة اتحولت لـ 'unknown')", before)
    return df


def build_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    dates = pd.date_range(orders["order_date"].min(), orders["order_date"].max(), freq="D")
    dim_date = pd.DataFrame({"date_id": dates})
    dim_date["year"] = dim_date["date_id"].dt.year
    dim_date["month"] = dim_date["date_id"].dt.month
    dim_date["day"] = dim_date["date_id"].dt.day
    dim_date["weekday_name"] = dim_date["date_id"].dt.day_name()
    dim_date["is_weekend"] = dim_date["date_id"].dt.weekday.isin([4, 5]).astype(int)
    dim_date["quarter"] = dim_date["date_id"].dt.quarter
    dim_date["date_id"] = dim_date["date_id"].dt.date
    return dim_date


def build_fact_sales(orders: pd.DataFrame, order_items: pd.DataFrame,
                      products: pd.DataFrame) -> pd.DataFrame:
    """يدمج الطلبات مع عناصرها ويحسب total_amount لكل سطر مبيعات."""
    df = order_items.merge(orders[["order_id", "customer_id", "order_date", "status"]],
                            on="order_id", how="inner")
    df = df.merge(products[["product_id", "unit_price"]], on="product_id", how="inner")

    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df["date_id"] = pd.to_datetime(df["order_date"]).dt.date

    fact = df[["item_id", "order_id", "customer_id", "product_id",
               "date_id", "quantity", "unit_price", "total_amount", "status"]].copy()

    logger.info("build_fact_sales: تم بناء %d صف في جدول الحقائق", len(fact))
    return fact


def transform_all(raw: dict) -> dict:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    dim_customer = clean_customers(raw["customers"])
    dim_product = clean_products(raw["products"])
    orders_clean = clean_orders(raw["orders"])
    dim_date = build_dim_date(orders_clean)
    fact_sales = build_fact_sales(orders_clean, raw["order_items"], dim_product)

    return {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }


if __name__ == "__main__":
    from extract import extract_all
    raw = extract_all()
    out = transform_all(raw)
    for k, v in out.items():
        print(f"{k}: {v.shape}")
