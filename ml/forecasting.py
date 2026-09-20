"""
forecasting.py
---------------
نموذج تنبؤ بالمبيعات اليومية المستقبلية (Time Series Forecasting).

المنهج:
  - نبني Features من التاريخ (day_of_week, month, is_weekend, lag features,
    rolling averages) بدل ما نستخدم مكتبة Time-series متخصصة (عشان مش متاحة
    هنا)، وده أسلوب شائع وفعّال جداً في الصناعة (Feature-based forecasting).
  - نستخدم Gradient Boosting Regressor (من scikit-learn) للتنبؤ.
  - نقيّم الأداء بـ MAE / RMSE / MAPE على بيانات مستقبلية (Time-based split،
    مش split عشوائي، عشان نتجنب data leakage من المستقبل للماضي).
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUT_DIR, exist_ok=True)


def load_daily_sales() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT date_id, SUM(total_amount) AS revenue
        FROM fact_sales
        WHERE status = 'completed'
        GROUP BY date_id
        ORDER BY date_id
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date_id"])
    conn.close()
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("date_id").asfreq("D").fillna(0).reset_index()

    df["day_of_week"] = df["date_id"].dt.dayofweek
    df["month"] = df["date_id"].dt.month
    df["is_weekend"] = df["day_of_week"].isin([4, 5]).astype(int)
    df["day_of_year"] = df["date_id"].dt.dayofyear

    # Lag features (المبيعات في الأيام السابقة)
    for lag in [1, 7, 14]:
        df[f"lag_{lag}"] = df["revenue"].shift(lag)

    # Rolling average (متوسط متحرك)
    df["rolling_mean_7"] = df["revenue"].shift(1).rolling(7).mean()
    df["rolling_mean_30"] = df["revenue"].shift(1).rolling(30).mean()

    df = df.dropna().reset_index(drop=True)
    return df


def train_and_evaluate():
    print("تحميل بيانات المبيعات اليومية من قاعدة البيانات...")
    daily = load_daily_sales()
    featured = build_features(daily)

    feature_cols = ["day_of_week", "month", "is_weekend", "day_of_year",
                     "lag_1", "lag_7", "lag_14", "rolling_mean_7", "rolling_mean_30"]
    X = featured[feature_cols]
    y = featured["revenue"]

    # Time-based split: آخر 60 يوم للاختبار (زي الواقع - مفيش عشوائية)
    split_idx = len(featured) - 60
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    dates_test = featured["date_id"].iloc[split_idx:]

    print(f"بيانات التدريب: {len(X_train)} يوم | بيانات الاختبار: {len(X_test)} يوم")

    model = GradientBoostingRegressor(
        n_estimators=300, max_depth=3, learning_rate=0.05, random_state=42
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mape = np.mean(np.abs((y_test - preds) / y_test.replace(0, np.nan))) * 100

    print("\n--- نتائج التقييم على بيانات لم يراها النموذج (آخر 60 يوم) ---")
    print(f"MAE  (متوسط الخطأ المطلق) : {mae:,.2f}")
    print(f"RMSE (جذر متوسط مربع الخطأ): {rmse:,.2f}")
    print(f"MAPE (نسبة الخطأ المئوية) : {mape:.2f}%")

    # أهمية الـ Features
    importance = pd.Series(model.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print("\n--- أهمية العوامل في التنبؤ ---")
    print(importance.round(3).to_string())

    # رسم بياني: الفعلي مقابل المتوقع
    plt.figure(figsize=(12, 5))
    plt.plot(dates_test, y_test.values, label="المبيعات الفعلية", linewidth=2)
    plt.plot(dates_test, preds, label="المبيعات المتوقعة", linewidth=2, linestyle="--")
    plt.title("تنبؤ المبيعات اليومية - الفعلي مقابل المتوقع (آخر 60 يوم)")
    plt.xlabel("التاريخ")
    plt.ylabel("الإيراد اليومي")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, "forecast_actual_vs_predicted.png")
    plt.savefig(out_path, dpi=110)
    print(f"\nتم حفظ الرسم البياني في: {out_path}")

    return {"mae": mae, "rmse": rmse, "mape": mape, "model": model, "importance": importance}


if __name__ == "__main__":
    train_and_evaluate()
