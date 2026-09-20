"""
generate_reviews.py
---------------------
يولّد بيانات "غير منظمة/شبه منظمة" (unstructured/semi-structured) —
تقييمات نصية حرة من العملاء + JSON متداخل فيه metadata، زي ما
بيحصل لما تيجي تستخرج بيانات من نظام تاني (review platform, app logs).

ده بيمثل جزء "unstructured data" في الوظيفة: مش كل حاجة بتيجي جاهزة
في صفوف وأعمدة — أحياناً بييجي نص حر لازم نستخرج منه معنى.
"""

import json
import os
import random
import numpy as np
from datetime import datetime, timedelta

np.random.seed(7)
random.seed(7)

OUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(OUT_DIR, exist_ok=True)

POSITIVE_PHRASES = [
    "المنتج ده كان أحلى من المتوقع بجد",
    "جودة ممتازة والتوصيل كان سريع جداً",
    "تجربة شراء رائعة, هكرر التجربة تاني أكيد",
    "خدمة العملاء كانت متعاونة جداً معايا",
    "المنتج مطابق تماماً للصور والوصف",
    "سعر ممتاز مقارنة بالجودة, يستاهل فعلاً",
]

NEGATIVE_PHRASES = [
    "للأسف المنتج وصل متأخر جداً عن الميعاد",
    "الجودة مش زي ما كنت متوقع, حاسس اني اتغبنت",
    "وصلني تالف والتغليف كان سيء",
    "خدمة العملاء ماردتش عليا لمدة يومين",
    "المنتج مختلف عن الصورة اللي في الموقع",
    "سعره غالي جداً بالنسبة لجودته",
]

NEUTRAL_PHRASES = [
    "المنتج عادي, مفيش حاجة مميزة فيه",
    "التوصيل خد وقته المعتاد, مفيش تأخير",
    "زي ما هو متوقع, مفيش مفاجآت",
]

DEVICES = ["mobile_app_ios", "mobile_app_android", "web_desktop", "web_mobile"]


def build_review(review_id: int, product_id: int, customer_id: int, order_date: datetime) -> dict:
    sentiment_bucket = np.random.choice(["positive", "negative", "neutral"], p=[0.55, 0.25, 0.20])
    phrases = {"positive": POSITIVE_PHRASES, "negative": NEGATIVE_PHRASES, "neutral": NEUTRAL_PHRASES}[sentiment_bucket]

    n_sentences = np.random.randint(1, 3)
    text = " ".join(random.sample(phrases, min(n_sentences, len(phrases))))

    rating_map = {"positive": [4, 5], "negative": [1, 2], "neutral": [3]}
    rating = int(np.random.choice(rating_map[sentiment_bucket]))

    review_date = order_date + timedelta(days=int(np.random.randint(1, 14)))

    # JSON متداخل (semi-structured) زي ما بييجي من API حقيقي
    return {
        "review_id": review_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "review_date": review_date.strftime("%Y-%m-%d"),
        "rating": rating,
        "review_text": text,
        "metadata": {
            "device": random.choice(DEVICES),
            "verified_purchase": bool(np.random.choice([True, False], p=[0.85, 0.15])),
            "helpful_votes": int(np.random.poisson(2)),
            "images_attached": int(np.random.choice([0, 0, 0, 1, 2], p=[0.5, 0.2, 0.15, 0.1, 0.05])),
        },
    }


def main():
    import pandas as pd

    orders_path = os.path.join(OUT_DIR, "orders.csv")
    items_path = os.path.join(OUT_DIR, "order_items.csv")

    if not (os.path.exists(orders_path) and os.path.exists(items_path)):
        raise FileNotFoundError("شغّل generate_raw_data.py الأول عشان نولّد الطلبات.")

    orders = pd.read_csv(orders_path, parse_dates=["order_date"])
    items = pd.read_csv(items_path)

    completed = orders[orders["status"] == "completed"]
    merged = items.merge(completed[["order_id", "customer_id", "order_date"]], on="order_id")

    # نختار عينة عشوائية من عمليات الشراء عشان نكتب عليها تقييمات
    # (مش كل عملية شراء بيتكتب عليها تقييم - ده واقعي, نسبة الـ review rate عادة 15-25%)
    sample = merged.sample(frac=0.18, random_state=7).reset_index(drop=True)

    reviews = []
    for i, row in enumerate(sample.itertuples(), start=1):
        reviews.append(build_review(i, int(row.product_id), int(row.customer_id), row.order_date))

    out_path = os.path.join(OUT_DIR, "customer_reviews.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(reviews, f, ensure_ascii=False, indent=2)

    print(f"تم توليد {len(reviews)} تقييم نصي (unstructured) في: {out_path}")


if __name__ == "__main__":
    main()
