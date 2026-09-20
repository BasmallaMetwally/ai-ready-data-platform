"""
train_and_save_models.py
--------------------------
يدرب كل نماذج الـ ML الثلاثة ويحفظهم على القرص (joblib) عشان الـ API
تقدر تحمّلهم مرة واحدة وتستخدمهم للتنبؤ الفوري (Inference) من غير ما
تعيد التدريب في كل request — ده أسلوب الإنتاج الصحيح (Production ML).
"""

import os
import sys
import joblib

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ml"))

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
os.makedirs(MODELS_DIR, exist_ok=True)


def train_and_save_forecasting():
    from forecasting import load_daily_sales, build_features
    from sklearn.ensemble import GradientBoostingRegressor

    daily = load_daily_sales()
    featured = build_features(daily)

    feature_cols = ["day_of_week", "month", "is_weekend", "day_of_year",
                     "lag_1", "lag_7", "lag_14", "rolling_mean_7", "rolling_mean_30"]
    X, y = featured[feature_cols], featured["revenue"]

    model = GradientBoostingRegressor(n_estimators=300, max_depth=3, learning_rate=0.05, random_state=42)
    model.fit(X, y)

    joblib.dump({"model": model, "feature_cols": feature_cols}, os.path.join(MODELS_DIR, "forecast_model.joblib"))
    # بنحفظ كمان آخر صف بيانات (بآخر lag/rolling values) عشان الـ API تقدر تبني الـ features للتنبؤ المستقبلي
    featured.to_parquet(os.path.join(MODELS_DIR, "forecast_history.parquet"), index=False) \
        if _has_parquet_support() else featured.to_csv(os.path.join(MODELS_DIR, "forecast_history.csv"), index=False)

    print("✅ تم حفظ نموذج التنبؤ في models/forecast_model.joblib")


def _has_parquet_support():
    try:
        import pyarrow  # noqa
        return True
    except ImportError:
        return False


def train_and_save_segmentation():
    from customer_segmentation import build_rfm, find_optimal_k, label_segments
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans

    rfm = build_rfm()
    features = rfm[["recency", "frequency", "monetary"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    best_k, _ = find_optimal_k(X_scaled)
    km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    rfm["cluster"] = km.fit_predict(X_scaled)
    rfm = label_segments(rfm)

    joblib.dump({"scaler": scaler, "kmeans": km}, os.path.join(MODELS_DIR, "segmentation_model.joblib"))
    rfm.to_csv(os.path.join(MODELS_DIR, "customer_segments_lookup.csv"), index=False)

    print("✅ تم حفظ نموذج تقسيم العملاء في models/segmentation_model.joblib")


def train_and_save_anomaly():
    from anomaly_detection import load_daily_metrics, detect_anomalies

    daily = load_daily_metrics()
    result = detect_anomalies(daily)
    result.to_csv(os.path.join(MODELS_DIR, "anomaly_results_lookup.csv"), index=False)

    print("✅ تم حفظ نتائج كشف الشذوذ في models/anomaly_results_lookup.csv")


def train_and_save_recommendations():
    # BUG FIX (found while merging): this function was never called from
    # __main__ below, so `train_and_save_models.py` — despite its name and
    # docstring promising "all three models" — silently never retrained the
    # recommendation model. The API would keep serving a stale
    # recommendation_model.joblib after every retrain. recommendation_system
    # already has its own build_and_save(); we just need to call it here too.
    from recommendation_system import build_and_save

    build_and_save()


if __name__ == "__main__":
    print("بدء تدريب وحفظ كل النماذج...")
    train_and_save_forecasting()
    train_and_save_segmentation()
    train_and_save_anomaly()
    train_and_save_recommendations()
    print("\nتم حفظ كل النماذج (forecast, segmentation, anomaly, recommendation) في مجلد models/ بنجاح.")
