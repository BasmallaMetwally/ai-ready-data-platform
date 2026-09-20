"""
customer_segmentation.py
--------------------------
تقسيم العملاء لمجموعات (Segments) باستخدام تحليل RFM
(Recency, Frequency, Monetary) + K-Means Clustering.

ده من أهم تطبيقات الـ ML في التسويق: بيسمح للشركة تستهدف كل مجموعة
عملاء باستراتيجية مختلفة (VIP, at-risk, new, etc.)
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUT_DIR, exist_ok=True)


def build_rfm() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT f.customer_id, f.date_id, f.order_id, f.total_amount
        FROM fact_sales f
        WHERE f.status = 'completed'
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date_id"])
    conn.close()

    snapshot_date = df["date_id"].max() + pd.Timedelta(days=1)

    rfm = df.groupby("customer_id").agg(
        recency=("date_id", lambda x: (snapshot_date - x.max()).days),
        frequency=("order_id", "nunique"),
        monetary=("total_amount", "sum"),
    ).reset_index()

    return rfm


def find_optimal_k(X_scaled, k_range=range(2, 8)):
    """يستخدم Silhouette Score عشان يختار أفضل عدد Clusters."""
    scores = {}
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        scores[k] = silhouette_score(X_scaled, labels)
    best_k = max(scores, key=scores.get)
    return best_k, scores


def label_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """يدي كل Cluster اسم واضح بناءً على متوسط RFM بتاعه."""
    cluster_summary = rfm.groupby("cluster")[["recency", "frequency", "monetary"]].mean()

    names = {}
    monetary_rank = cluster_summary["monetary"].rank(ascending=False)
    recency_rank = cluster_summary["recency"].rank(ascending=True)  # أقل recency = أنشط

    for cid in cluster_summary.index:
        if monetary_rank[cid] == 1 and recency_rank[cid] <= 2:
            names[cid] = "VIP - عملاء بيإنفقوا كتير ونشطين"
        elif recency_rank[cid] == cluster_summary.shape[0]:
            names[cid] = "At Risk - عملاء مش نشطين من فترة"
        elif monetary_rank[cid] <= 2:
            names[cid] = "Loyal - عملاء منتظمين"
        else:
            names[cid] = "Casual - عملاء عاديين"

    rfm["segment"] = rfm["cluster"].map(names)
    return rfm


def run_segmentation():
    print("بناء مقاييس RFM لكل عميل...")
    rfm = build_rfm()
    print(f"عدد العملاء: {len(rfm)}")

    features = rfm[["recency", "frequency", "monetary"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    print("اختيار أفضل عدد Clusters باستخدام Silhouette Score...")
    best_k, scores = find_optimal_k(X_scaled)
    print("Silhouette scores:", {k: round(v, 3) for k, v in scores.items()})
    print(f"أفضل عدد Clusters: {best_k}")

    km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    rfm["cluster"] = km.fit_predict(X_scaled)

    rfm = label_segments(rfm)

    print("\n--- ملخص كل شريحة عملاء ---")
    summary = rfm.groupby("segment").agg(
        n_customers=("customer_id", "count"),
        avg_recency_days=("recency", "mean"),
        avg_frequency=("frequency", "mean"),
        avg_monetary=("monetary", "mean"),
    ).round(1).sort_values("avg_monetary", ascending=False)
    print(summary.to_string())

    # رسم بياني: توزيع العملاء (Frequency vs Monetary ملون بالشريحة)
    plt.figure(figsize=(9, 6))
    for seg in rfm["segment"].unique():
        subset = rfm[rfm["segment"] == seg]
        plt.scatter(subset["frequency"], subset["monetary"], label=seg, alpha=0.6, s=25)
    plt.xlabel("Frequency (عدد الطلبات)")
    plt.ylabel("Monetary (إجمالي الإنفاق)")
    plt.title("تقسيم العملاء حسب سلوك الشراء (RFM Segmentation)")
    plt.legend()
    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, "customer_segments.png")
    plt.savefig(out_path, dpi=110)
    print(f"\nتم حفظ الرسم البياني في: {out_path}")

    rfm.to_csv(os.path.join(OUT_DIR, "customer_segments.csv"), index=False)
    return rfm, summary


if __name__ == "__main__":
    run_segmentation()
