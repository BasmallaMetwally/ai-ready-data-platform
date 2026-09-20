"""
Generates a sample dataset (customers_orders.csv) with real, deliberately
injected data quality problems, to test the validation system against:
- Missing values
- Duplicate rows / duplicate primary keys
- Numeric outliers
- Schema violations (text in a numeric column, out-of-range values)
- Consistency issues (end date before start date, malformed email)
"""
import pandas as pd
import numpy as np
import random

random.seed(42)
np.random.seed(42)

n = 500
rows = []

first_names = ["Ahmed", "Sara", "Mohamed", "Nour", "Omar", "Mona", "Khaled", "Yara", "Tarek", "Dina"]
last_names = ["Hassan", "Ali", "Ibrahim", "Fathy", "Sami", "Adel", "Nasser", "Kamal"]

for i in range(1, n + 1):
    customer_id = i
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    age = np.random.randint(18, 70)
    email = f"{name.lower().replace(' ', '.')}{i}@example.com"
    signup_date = pd.Timestamp("2023-01-01") + pd.Timedelta(days=random.randint(0, 700))
    order_amount = round(np.random.normal(500, 150), 2)
    country = random.choice(["Egypt", "UAE", "Saudi Arabia", "Jordan", "Qatar"])
    order_start = signup_date + pd.Timedelta(days=random.randint(0, 30))
    order_end = order_start + pd.Timedelta(days=random.randint(1, 10))

    rows.append({
        "customer_id": customer_id,
        "name": name,
        "age": age,
        "email": email,
        "signup_date": signup_date,
        "order_amount": order_amount,
        "country": country,
        "order_start": order_start,
        "order_end": order_end,
    })

df = pd.DataFrame(rows)

# --- Inject deliberate problems ---

# 1) Missing values
missing_idx_email = np.random.choice(df.index, size=25, replace=False)
df.loc[missing_idx_email, "email"] = np.nan

missing_idx_age = np.random.choice(df.index, size=15, replace=False)
df.loc[missing_idx_age, "age"] = np.nan

missing_idx_country = np.random.choice(df.index, size=10, replace=False)
df.loc[missing_idx_country, "country"] = np.nan

# 2) Duplicate rows (copy whole rows)
dup_rows = df.sample(8, random_state=1)
df = pd.concat([df, dup_rows], ignore_index=True)

# 3) Duplicate primary key (customer_id repeated with different values)
dup_key_idx = np.random.choice(df.index, size=5, replace=False)
df.loc[dup_key_idx, "customer_id"] = df.loc[dup_key_idx, "customer_id"].values[0]

# 4) Numeric outliers
outlier_idx = np.random.choice(df.index, size=6, replace=False)
df.loc[outlier_idx, "order_amount"] = [9999.0, -500.0, 15000.0, 8800.0, -200.0, 12000.0]

# 5) Schema violations
df["age"] = df["age"].astype(object)  # so we can inject text values into the age column
schema_idx = np.random.choice(df.index, size=5, replace=False)
df.loc[schema_idx[:3], "age"] = ["thirty", "N/A", "unknown"]  # text in a numeric column
df.loc[schema_idx[3:], "age"] = [-5, 200]  # values outside a sane range

# 6) Consistency issues: end date before start date
inconsistent_idx = np.random.choice(df.index, size=7, replace=False)
df.loc[inconsistent_idx, "order_end"] = df.loc[inconsistent_idx, "order_start"] - pd.Timedelta(days=3)

# 7) Malformed emails
bad_email_idx = np.random.choice(df.index, size=8, replace=False)
df.loc[bad_email_idx, "email"] = ["not-an-email", "test@", "abc.com", "user@@x.com",
                                    "hello world@x.com", "x@y", "plainaddress", "@missinguser.com"]

df.to_csv("customers_orders.csv", index=False)
print(f"Sample dataset generated: {len(df)} rows")
print(df.head())
