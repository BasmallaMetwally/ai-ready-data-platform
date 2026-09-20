"""
extract.py
----------
طبقة الاستخراج (Extract) في الـ ETL Pipeline.
مسؤولة عن قراءة البيانات الخام من مصادرها (هنا: ملفات CSV،
لكن في مشروع حقيقي ممكن تكون API أو قاعدة بيانات تشغيلية أخرى).
"""

import os
import pandas as pd
import logging

logger = logging.getLogger("etl.extract")

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def extract_all() -> dict:
    """يقرأ كل ملفات الـ CSV الخام ويرجعهم كـ dict من DataFrames."""
    logger.info("بدء استخراج البيانات من: %s", RAW_DIR)

    tables = {}
    for name in ["customers", "products", "orders", "order_items"]:
        path = os.path.join(RAW_DIR, f"{name}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"الملف {path} غير موجود. شغّل data/generate_raw_data.py أولاً."
            )
        df = pd.read_csv(path)
        logger.info("  تم تحميل %-14s -> %6d صف", name, len(df))
        tables[name] = df

    return tables


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    data = extract_all()
    for k, v in data.items():
        print(f"{k}: {v.shape}")
