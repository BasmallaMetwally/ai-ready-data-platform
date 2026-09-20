"""
services.py
-----------
طبقة الـ Business Logic بتاعة الـ API — منفصلة عن FastAPI نفسه عشان:
  1) تقدر تتختبر (Unit Testing) من غير ما تشغّل سيرفر
  2) لو حبينا نغير الـ framework (Flask بدل FastAPI مثلاً) الكود ده مايتغيرش

كل دالة هنا بترجع Python dict/list عادي، وFastAPI في main.py بس بيغلفها
في response models ويعرضها كـ JSON.
"""

import os
import sqlite3
import numpy as np
import pandas as pd
import joblib

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")

_forecast_bundle = None
_segmentation_lookup = None
_anomaly_lookup = None
_recommendation_bundle = None


def _load_forecast_bundle():
    global _forecast_bundle
    if _forecast_bundle is None:
        _forecast_bundle = joblib.load(os.path.join(MODELS_DIR, "forecast_model.joblib"))
        hist_path_csv = os.path.join(MODELS_DIR, "forecast_history.csv")
        hist_path_parquet = os.path.join(MODELS_DIR, "forecast_history.parquet")
        if os.path.exists(hist_path_parquet):
            _forecast_bundle["history"] = pd.read_parquet(hist_path_parquet)
        else:
            _forecast_bundle["history"] = pd.read_csv(hist_path_csv, parse_dates=["date_id"])
    return _forecast_bundle


def _load_segmentation_lookup():
    global _segmentation_lookup
    if _segmentation_lookup is None:
        _segmentation_lookup = pd.read_csv(os.path.join(MODELS_DIR, "customer_segments_lookup.csv"))
    return _segmentation_lookup


def _load_anomaly_lookup():
    global _anomaly_lookup
    if _anomaly_lookup is None:
        _anomaly_lookup = pd.read_csv(os.path.join(MODELS_DIR, "anomaly_results_lookup.csv"), parse_dates=["date_id"])
    return _anomaly_lookup


def _load_recommendation_bundle():
    global _recommendation_bundle
    if _recommendation_bundle is None:
        _recommendation_bundle = joblib.load(os.path.join(MODELS_DIR, "recommendation_model.joblib"))
    return _recommendation_bundle


# ---------------------------------------------------------------
# Health
# ---------------------------------------------------------------
def health_check() -> dict:
    db_ok = os.path.exists(DB_PATH)
    models_ok = os.path.exists(os.path.join(MODELS_DIR, "forecast_model.joblib"))
    return {"status": "ok" if (db_ok and models_ok) else "degraded",
            "database": db_ok, "models_loaded": models_ok}


# ---------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------
def forecast_next_days(n_days: int = 7) -> list:
    bundle = _load_forecast_bundle()
    model, feature_cols, history = bundle["model"], bundle["feature_cols"], bundle["history"].copy()

    results = []
    for _ in range(n_days):
        last_row = history.iloc[-1]
        next_date = last_row["date_id"] + pd.Timedelta(days=1)

        row = {
            "day_of_week": next_date.dayofweek,
            "month": next_date.month,
            "is_weekend": int(next_date.dayofweek in [4, 5]),
            "day_of_year": next_date.dayofyear,
            "lag_1": last_row["revenue"],
            "lag_7": history.iloc[-7]["revenue"] if len(history) >= 7 else last_row["revenue"],
            "lag_14": history.iloc[-14]["revenue"] if len(history) >= 14 else last_row["revenue"],
            "rolling_mean_7": history["revenue"].tail(7).mean(),
            "rolling_mean_30": history["revenue"].tail(30).mean(),
        }
        X_pred = pd.DataFrame([row])[feature_cols]
        pred = float(model.predict(X_pred)[0])

        results.append({"date": next_date.strftime("%Y-%m-%d"), "predicted_revenue": round(pred, 2)})

        # نضيف التنبؤ للتاريخ عشان الـ lag/rolling بتاع اليوم اللي بعده يتحسب صح
        history = pd.concat([history, pd.DataFrame([{"date_id": next_date, "revenue": pred}])], ignore_index=True)

    return results


# ---------------------------------------------------------------
# Customer Segmentation
# ---------------------------------------------------------------
def get_customer_segment(customer_id: int) -> dict:
    lookup = _load_segmentation_lookup()
    row = lookup[lookup["customer_id"] == customer_id]
    if row.empty:
        return None
    r = row.iloc[0]
    return {
        "customer_id": int(r["customer_id"]),
        "segment": r["segment"],
        "recency_days": int(r["recency"]),
        "frequency": int(r["frequency"]),
        "monetary": round(float(r["monetary"]), 2),
    }


def list_segments_summary() -> list:
    lookup = _load_segmentation_lookup()
    summary = lookup.groupby("segment").agg(
        n_customers=("customer_id", "count"),
        avg_recency=("recency", "mean"),
        avg_frequency=("frequency", "mean"),
        avg_monetary=("monetary", "mean"),
    ).round(2).reset_index()
    return summary.to_dict(orient="records")


# ---------------------------------------------------------------
# Anomaly Detection
# ---------------------------------------------------------------
def get_recent_anomalies(limit: int = 10) -> list:
    lookup = _load_anomaly_lookup()
    anomalies = lookup[lookup["anomaly"] == -1].sort_values("date_id", ascending=False).head(limit)
    return [
        {"date": row["date_id"].strftime("%Y-%m-%d"),
         "revenue": round(float(row["revenue"]), 2),
         "n_orders": int(row["n_orders"]),
         "anomaly_score": round(float(row["anomaly_score"]), 4)}
        for _, row in anomalies.iterrows()
    ]


# ---------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------
def recommend_similar_products(product_id: int, top_n: int = 5) -> list:
    bundle = _load_recommendation_bundle()
    sim_matrix, product_ids = bundle["sim_matrix"], bundle["product_ids"]

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


def recommend_for_customer(customer_id: int, top_n: int = 5) -> list:
    bundle = _load_recommendation_bundle()
    sim_matrix, product_ids, df = bundle["sim_matrix"], bundle["product_ids"], bundle["purchase_df"]

    bought = df[df["customer_id"] == customer_id]["product_id"].unique()
    if len(bought) == 0:
        return []

    bought_idx = [np.where(product_ids == p)[0][0] for p in bought if p in product_ids]
    if not bought_idx:
        return []

    agg_scores = sim_matrix[bought_idx].mean(axis=0)
    ranked_idx = np.argsort(agg_scores)[::-1]

    recs = []
    for i in ranked_idx:
        pid = int(product_ids[i])
        if pid in bought:
            continue
        recs.append({"product_id": pid, "score": round(float(agg_scores[i]), 4)})
        if len(recs) >= top_n:
            break
    return recs
