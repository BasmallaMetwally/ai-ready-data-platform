"""
generate_raw_data.py
---------------------
بيولد بيانات خام (raw) تحاكي مصدر بيانات حقيقي لمتجر إلكتروني:
- عملاء (customers)
- منتجات (products)
- طلبات (orders)
- عناصر الطلبات (order_items)

البيانات متعمد فيها بعض "الوسخ" (missing values, duplicates, outliers)
عشان الـ ETL يبقى له معنى حقيقي (زي ما بيحصل في الواقع).
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------------------------------------------------
# 1. Customers
# ---------------------------------------------------------------
N_CUSTOMERS = 2000
CITIES = ["Cairo", "Alexandria", "Giza", "Damietta", "Mansoura",
          "Tanta", "Aswan", "Luxor", "Suez", "Port Said"]

first_names = ["Ahmed", "Mohamed", "Sara", "Mona", "Ali", "Youssef", "Nour",
               "Fatma", "Omar", "Laila", "Khaled", "Hana", "Amr", "Salma"]
last_names = ["Hassan", "Mahmoud", "Ibrahim", "Said", "Abdallah", "Fathy",
              "Kamal", "Ramzy", "Adel", "Fawzy"]

customers = pd.DataFrame({
    "customer_id": range(1, N_CUSTOMERS + 1),
    "name": [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(N_CUSTOMERS)],
    "city": np.random.choice(CITIES, N_CUSTOMERS, p=[0.25, 0.15, 0.12, 0.08, 0.08,
                                                       0.08, 0.06, 0.06, 0.06, 0.06]),
    "signup_date": [
        (datetime(2023, 1, 1) + timedelta(days=int(x)))
        for x in np.random.randint(0, 900, N_CUSTOMERS)
    ],
    "age": np.random.randint(18, 65, N_CUSTOMERS).astype(float),
})

# نضيف بعض القيم الناقصة عمداً (data quality issue واقعية)
missing_age_idx = np.random.choice(customers.index, size=int(N_CUSTOMERS * 0.05), replace=False)
customers.loc[missing_age_idx, "age"] = np.nan

# بعض التكرار (duplicate rows) زي ما بيحصل من أخطاء الإدخال
dupes = customers.sample(15, random_state=1)
customers = pd.concat([customers, dupes], ignore_index=True)

customers.to_csv(os.path.join(OUT_DIR, "customers.csv"), index=False)

# ---------------------------------------------------------------
# 2. Products
# ---------------------------------------------------------------
CATEGORIES = {
    "Electronics": (200, 15000),
    "Fashion": (50, 2000),
    "Home & Kitchen": (30, 5000),
    "Beauty": (20, 800),
    "Sports": (50, 3000),
    "Books": (20, 400),
    "Toys": (30, 1200),
}

N_PRODUCTS = 300
product_rows = []
pid = 1
for _ in range(N_PRODUCTS):
    cat = random.choice(list(CATEGORIES.keys()))
    lo, hi = CATEGORIES[cat]
    price = round(np.random.uniform(lo, hi), 2)
    product_rows.append({
        "product_id": pid,
        "product_name": f"{cat}_item_{pid}",
        "category": cat,
        "unit_price": price,
        "cost_price": round(price * np.random.uniform(0.5, 0.8), 2),
    })
    pid += 1

products = pd.DataFrame(product_rows)
products.to_csv(os.path.join(OUT_DIR, "products.csv"), index=False)

# ---------------------------------------------------------------
# 3. Orders + Order items (مع اتجاه نمو + موسمية + بعض الشذوذ)
# ---------------------------------------------------------------
START_DATE = datetime(2023, 1, 1)
N_DAYS = 900  # حوالي سنتين وشوية عشان الـ forecasting يبقى له معنى

order_rows = []
item_rows = []
order_id = 1
item_id = 1

valid_customer_ids = customers["customer_id"].unique()
valid_product_ids = products["product_id"].values

for day_offset in range(N_DAYS):
    current_date = START_DATE + timedelta(days=day_offset)

    # اتجاه نمو تدريجي + موسمية أسبوعية (الجمعة والسبت أعلى) + موسمية شهرية (نوفمبر/ديسمبر أعلى - بلاك فرايدي)
    trend = 1 + (day_offset / N_DAYS) * 1.5
    weekday_factor = 1.6 if current_date.weekday() in [4, 5] else 1.0
    season_factor = 2.2 if current_date.month in [11, 12] else 1.0
    noise = np.random.normal(1, 0.15)

    base_orders = 15
    n_orders_today = max(1, int(base_orders * trend * weekday_factor * season_factor * noise))

    # حقن بعض الـ anomalies (أيام فيها عطل مفاجئ في المبيعات أو ارتفاع غير طبيعي - عشان anomaly detection)
    if random.random() < 0.01:
        n_orders_today = int(n_orders_today * 5)  # يوم فيه عرض/فلاش سيل غريب
    if random.random() < 0.01:
        n_orders_today = max(1, int(n_orders_today * 0.1))  # يوم فيه عطل بالموقع

    for _ in range(n_orders_today):
        cust_id = int(np.random.choice(valid_customer_ids))
        status = np.random.choice(
            ["completed", "completed", "completed", "cancelled", "returned"],
            p=[0.75, 0.1, 0.05, 0.05, 0.05]
        ) if False else np.random.choice(
            ["completed", "cancelled", "returned"], p=[0.85, 0.08, 0.07]
        )

        order_rows.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_date": current_date.strftime("%Y-%m-%d"),
            "status": status,
        })

        n_items = np.random.randint(1, 5)
        chosen_products = np.random.choice(valid_product_ids, n_items, replace=False)
        for p in chosen_products:
            qty = np.random.randint(1, 4)
            item_rows.append({
                "item_id": item_id,
                "order_id": order_id,
                "product_id": int(p),
                "quantity": int(qty),
            })
            item_id += 1

        order_id += 1

orders = pd.DataFrame(order_rows)
order_items = pd.DataFrame(item_rows)

# بعض القيم الناقصة في status (data quality issue)
missing_status_idx = np.random.choice(orders.index, size=int(len(orders) * 0.02), replace=False)
orders.loc[missing_status_idx, "status"] = np.nan

orders.to_csv(os.path.join(OUT_DIR, "orders.csv"), index=False)
order_items.to_csv(os.path.join(OUT_DIR, "order_items.csv"), index=False)

print("تم توليد البيانات الخام بنجاح:")
print(f"  customers.csv    -> {len(customers):,} صف")
print(f"  products.csv     -> {len(products):,} صف")
print(f"  orders.csv       -> {len(orders):,} صف")
print(f"  order_items.csv  -> {len(order_items):,} صف")
