"""
recommendation_system.py
---------------------------
نظام توصية بالمنتجات (Recommendation System) باستخدام أسلوب
Item-Based Collaborative Filtering: "العملاء اللي اشتروا المنتج ده
اشتروا كمان إيه؟" — بنحسب تشابه المنتجات بناءً على Co-purchase
patterns (Cosine Similarity على مصفوفة عميل × منتج).

ده نفس المبدأ اللي بتستخدمه أمازون في "Customers who bought this
item also bought".
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
os.makedirs(MODELS_DIR, exist_ok=True)


def load_customer_product_matrix() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT customer_id, product_id, SUM(quantity) AS qty
        FROM fact_sales
        WHERE status = 'completed'
        GROUP BY customer_id, product_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def build_item_similarity(df: pd.DataFrame):
    """يبني مصفوفة تشابه بين المنتجات (Product x Product) بناءً على أنماط الشراء المشترك."""
    customers = df["customer_id"].astype("category")
    products = df["product_id"].astype("category")

    matrix = csr_matrix(
        (df["qty"], (products.cat.codes, customers.cat.codes)),
        shape=(len(products.cat.categories), len(customers.cat.categories)),
    )

    # Cosine similarity بين المنتجات (صف لكل منتج)
    sim_matrix = cosine_similarity(matrix)
    product_ids = products.cat.categories.to_numpy()

    return sim_matrix, product_ids


def recommend_similar_products(product_id: int, sim_matrix, product_ids, top_n: int = 5) -> list:
    """يرجع أشبه N منتج للمنتج المُعطى، بناءً على أنماط الشراء المشترك."""
    if product_id not in product_ids:
        return []

    idx = np.where(product_ids == product_id)[0][0]
    scores = sim_matrix[idx]

    similar_idx = np.argsort(scores)[::-1]
    similar_idx = [i for i in similar_idx if i != idx][:top_n]

    return [
        {"product_id": int(product_ids[i]), "similarity_score": round(float(scores[i]), 4)}
        for i in similar_idx
    ]


def recommend_for_customer(customer_id: int, df: pd.DataFrame, sim_matrix, product_ids, top_n: int = 5) -> list:
    """توصية شخصية لعميل: بيجمع تشابه كل المنتجات اللي اشتراها قبل كده."""
    bought = df[df["customer_id"] == customer_id]["product_id"].unique()
    if len(bought) == 0:
        return []

    bought_idx = [np.where(product_ids == p)[0][0] for p in bought if p in product_ids]
    if not bought_idx:
        return []

    agg_scores = sim_matrix[bought_idx].mean(axis=0)
    ranked_idx = np.argsort(agg_scores)[::-1]

    recommendations = []
    for i in ranked_idx:
        pid = int(product_ids[i])
        if pid in bought:
            continue
        recommendations.append({"product_id": pid, "score": round(float(agg_scores[i]), 4)})
        if len(recommendations) >= top_n:
            break

    return recommendations


def build_and_save():
    print("تحميل مصفوفة العميل × المنتج من قاعدة البيانات...")
    df = load_customer_product_matrix()
    print(f"عدد العملاء الفريدين: {df['customer_id'].nunique()} | عدد المنتجات: {df['product_id'].nunique()}")

    print("بناء مصفوفة تشابه المنتجات (Item-Item Similarity)...")
    sim_matrix, product_ids = build_item_similarity(df)

    import joblib
    joblib.dump(
        {"sim_matrix": sim_matrix, "product_ids": product_ids, "purchase_df": df},
        os.path.join(MODELS_DIR, "recommendation_model.joblib"),
    )
    print("✅ تم حفظ نموذج التوصية في models/recommendation_model.joblib")

    return sim_matrix, product_ids, df


if __name__ == "__main__":
    sim_matrix, product_ids, df = build_and_save()

    sample_product = int(product_ids[0])
    print(f"\n--- مثال: منتجات مشابهة للمنتج رقم {sample_product} ---")
    recs = recommend_similar_products(sample_product, sim_matrix, product_ids, top_n=5)
    for r in recs:
        print(f"  منتج {r['product_id']} - تشابه: {r['similarity_score']}")

    sample_customer = int(df["customer_id"].iloc[0])
    print(f"\n--- مثال: توصيات شخصية للعميل رقم {sample_customer} ---")
    recs = recommend_for_customer(sample_customer, df, sim_matrix, product_ids, top_n=5)
    for r in recs:
        print(f"  منتج {r['product_id']} - score: {r['score']}")
