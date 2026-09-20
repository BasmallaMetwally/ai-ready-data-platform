"""
anomaly_detection.py
----------------------
كشف الأيام "الشاذة" في المبيعات (فلاش سيل غير متوقع، عطل بالموقع، إلخ)
باستخدام Isolation Forest - خوارزمية غير خاضعة للإشراف (Unsupervised)
مخصصة لكشف الـ outliers.
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUT_DIR, exist_ok=True)


def load_daily_metrics() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT date_id,
               SUM(total_amount) AS revenue,
               COUNT(DISTINCT order_id) AS n_orders
        FROM fact_sales
        WHERE status = 'completed'
        GROUP BY date_id
        ORDER BY date_id
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date_id"])
    conn.close()
    return df


def detect_anomalies(df: pd.DataFrame, contamination: float = 0.02) -> pd.DataFrame:
    df = df.copy()
    df["day_of_week"] = df["date_id"].dt.dayofweek
    df["month"] = df["date_id"].dt.month

    # نشيل تأثير الاتجاه العام والموسمية الأسبوعية عشان نكشف الشذوذ الحقيقي
    # (مش بس "يوم جمعة فيه مبيعات أعلى" - ده طبيعي)
    df["revenue_zscore_by_weekday"] = df.groupby("day_of_week")["revenue"].transform(
        lambda x: (x - x.mean()) / x.std()
    )

    features = df[["revenue", "n_orders", "revenue_zscore_by_weekday"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    model = IsolationForest(contamination=contamination, random_state=42, n_estimators=200)
    df["anomaly"] = model.fit_predict(X_scaled)  # -1 = anomaly, 1 = normal
    df["anomaly_score"] = model.decision_function(X_scaled)

    return df


def run_anomaly_detection():
    print("تحميل بيانات المبيعات اليومية...")
    daily = load_daily_metrics()

    print("تشغيل Isolation Forest لكشف الأيام الشاذة...")
    result = detect_anomalies(daily)

    anomalies = result[result["anomaly"] == -1].sort_values("date_id")
    print(f"\nتم اكتشاف {len(anomalies)} يوم شاذ من أصل {len(result)} يوم:\n")
    print(anomalies[["date_id", "revenue", "n_orders", "anomaly_score"]]
          .assign(date_id=lambda x: x["date_id"].dt.date)
          .to_string(index=False))

    # تصنيف نوع الشذوذ: ارتفاع غير طبيعي ولا انخفاض غير طبيعي؟
    overall_mean = result["revenue"].mean()
    anomalies = anomalies.copy()
    anomalies["type"] = np.where(anomalies["revenue"] > overall_mean, "ارتفاع غير متوقع 📈", "انخفاض غير متوقع 📉")
    print("\n--- تصنيف الأيام الشاذة ---")
    print(anomalies[["date_id", "revenue", "type"]].assign(date_id=lambda x: x["date_id"].dt.date).to_string(index=False))

    # رسم بياني
    plt.figure(figsize=(14, 5))
    plt.plot(result["date_id"], result["revenue"], label="المبيعات اليومية", linewidth=1, color="steelblue")
    plt.scatter(anomalies["date_id"], anomalies["revenue"], color="red", s=60,
                label="أيام شاذة", zorder=5)
    plt.title("كشف الأيام الشاذة في المبيعات اليومية (Isolation Forest)")
    plt.xlabel("التاريخ")
    plt.ylabel("الإيراد اليومي")
    plt.legend()
    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, "anomaly_detection.png")
    plt.savefig(out_path, dpi=110)
    print(f"\nتم حفظ الرسم البياني في: {out_path}")

    result.to_csv(os.path.join(OUT_DIR, "daily_sales_with_anomalies.csv"), index=False)
    return result, anomalies


if __name__ == "__main__":
    run_anomaly_detection()
