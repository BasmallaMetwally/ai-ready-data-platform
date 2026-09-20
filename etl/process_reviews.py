"""
process_reviews.py
---------------------
معالجة بيانات "Unstructured" (تقييمات نصية + JSON متداخل):
  1) Flatten الـ JSON المتداخل لجدول مسطّح (metadata.device -> عمود مستقل)
  2) تنظيف النص العربي (إزالة علامات ترقيم، توحيد المسافات)
  3) استخراج ميزات (Feature Extraction) من النص: عدد الكلمات، وتحليل
     مشاعر بواحدة من طريقتين (اختر بـ --backend):
       - lexicon (افتراضي): قاموس كلمات إيجابية/سلبية، من غير أي
         dependency تقيلة أو اتصال إنترنت.
       - hf: نموذج multilingual من Hugging Face
         (cardiffnlp/twitter-xlm-roberta-base-sentiment، مُدرّب على نصوص
         متعددة اللغات بما فيها العربية). يحتاج `pip install -r
         requirements-nlp.txt` (transformers + torch) وتحميل النموذج من
         huggingface.co أول مرة تشغّله. لو الـ backend مش متاح لأي سبب
         (المكتبات مش متسطبة، أو مفيش إنترنت لتحميل النموذج)، بيرجع
         تلقائيًا لـ lexicon مع warning واضح — مبيفشلش الـ pipeline كله.
  4) تحميل الناتج كجدول جديد في الـ Data Warehouse: fact_reviews

ملحوظة عن التجربة الفعلية: بيئة البناء اللي كتبت فيها الكود فيها
`transformers` متسطب لكن من غير `torch` (تثبيته تقيل، بالـ GB)، ومفيش
اتصال بـ huggingface.co أصلاً. يعني قدرت أتأكد إن مسار الرجوع التلقائي
(fallback) شغال فعليًا — جرّبته وشفت الـ warning الصحيح يظهر ويكمل
بالـ lexicon من غير ما يوقّف — لكن مقدرتش أتأكد من دقة النموذج نفسه
(HF) ولا قارنته بالـ lexicon على عينة حقيقية. جرّب ده بنفسك لما يبقى عندك
نت واتصال بـ huggingface.co.
"""

import argparse
import json
import logging
import os
import re
import sqlite3

import pandas as pd

logger = logging.getLogger("etl.process_reviews")

RAW_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "customer_reviews.json")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "warehouse.db")

HF_MODEL_NAME = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
# Verify the model name/license yourself before depending on it in
# production — model cards change. As of writing it's a multilingual
# (incl. Arabic) RoBERTa sentiment classifier under the MIT license.

# قاموس بسيط لكلمات إيجابية/سلبية بالعامية المصرية - Lexicon-based sentiment
POSITIVE_WORDS = {"ممتاز", "رائعة", "أحلى", "سريع", "متعاونة", "مطابق", "يستاهل", "ممتازة"}
NEGATIVE_WORDS = {"متأخر", "للأسف", "تالف", "سيء", "غالي", "اتغبنت", "مختلف"}


def flatten_reviews(raw_reviews: list) -> pd.DataFrame:
    """يحول JSON المتداخل لجدول مسطّح - Flattening."""
    rows = []
    for r in raw_reviews:
        rows.append({
            "review_id": r["review_id"],
            "product_id": r["product_id"],
            "customer_id": r["customer_id"],
            "review_date": r["review_date"],
            "rating": r["rating"],
            "review_text": r["review_text"],
            # Flatten لكل حقول الـ metadata المتداخلة
            "device": r["metadata"]["device"],
            "verified_purchase": int(r["metadata"]["verified_purchase"]),
            "helpful_votes": r["metadata"]["helpful_votes"],
            "images_attached": r["metadata"]["images_attached"],
        })
    return pd.DataFrame(rows)


def clean_text(text: str) -> str:
    """تنظيف نص عربي: إزالة علامات ترقيم زيادة وتوحيد المسافات."""
    text = re.sub(r"[^\w\s\u0600-\u06FF]", " ", text)  # إزالة علامات الترقيم مع الحفاظ على العربي
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_sentiment_features_lexicon(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def score_sentiment(text: str) -> int:
        words = set(text.split())
        pos = len(words & POSITIVE_WORDS)
        neg = len(words & NEGATIVE_WORDS)
        return pos - neg

    df["sentiment_score"] = df["clean_text"].apply(score_sentiment)
    df["sentiment_label"] = pd.cut(
        df["sentiment_score"], bins=[-100, -1, 0, 100],
        labels=["negative", "neutral", "positive"]
    )
    return df


def _hf_backend_available() -> bool:
    """Checks the backend can actually run (libraries importable AND a
    modeling framework present) before attempting anything expensive."""
    try:
        import transformers  # noqa: F401
    except ImportError:
        logger.warning("transformers not installed — falling back to lexicon sentiment. "
                        "Install with: pip install -r requirements-nlp.txt")
        return False
    try:
        import torch  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import tensorflow  # noqa: F401
        return True
    except ImportError:
        pass
    logger.warning("transformers is installed but no backing framework (torch/tensorflow) is — "
                    "falling back to lexicon sentiment. Install with: pip install -r requirements-nlp.txt")
    return False


def extract_sentiment_features_hf(df: pd.DataFrame, model_name: str = HF_MODEL_NAME) -> "tuple[pd.DataFrame, str]":
    """Hugging Face multilingual sentiment classifier. Falls back to the
    lexicon method (logging why) if the library, framework, or the model
    download itself isn't available — this function never raises on a
    missing/unreachable model, by design, so a flaky network doesn't take
    the whole ETL pipeline down.

    Returns (df, backend_used) rather than just df: a bug in an earlier
    version logged "backend=hf" even when this had silently fallen back to
    the lexicon backend, which was misleading about what actually ran.
    Callers should log/report the returned backend name, not the one they
    requested."""
    if not _hf_backend_available():
        return extract_sentiment_features_lexicon(df), "lexicon"

    from transformers import pipeline

    try:
        classifier = pipeline("sentiment-analysis", model=model_name)
    except Exception:
        logger.exception(
            "Could not load the Hugging Face model '%s' (likely no network access to "
            "huggingface.co, or the model was renamed/removed) — falling back to lexicon sentiment.",
            model_name,
        )
        return extract_sentiment_features_lexicon(df), "lexicon"

    df = df.copy()
    texts = df["clean_text"].fillna("").tolist()
    # batch to keep memory bounded on large review sets
    results = []
    batch_size = 32
    for i in range(0, len(texts), batch_size):
        results.extend(classifier(texts[i:i + batch_size], truncation=True))

    label_map = {"positive": "positive", "negative": "negative", "neutral": "neutral",
                 "LABEL_0": "negative", "LABEL_1": "neutral", "LABEL_2": "positive"}
    df["sentiment_label"] = [label_map.get(r["label"].lower() if isinstance(r["label"], str) else r["label"],
                                            label_map.get(r["label"], "neutral"))
                              for r in results]
    df["sentiment_score"] = [round(r["score"], 4) for r in results]
    return df, "hf"


def extract_sentiment_features(df: pd.DataFrame, backend: str = "lexicon") -> "tuple[pd.DataFrame, str]":
    """Returns (df, backend_used) — backend_used may differ from the
    requested `backend` if hf was requested but unavailable (see
    extract_sentiment_features_hf's docstring)."""
    df = df.copy()
    df["clean_text"] = df["review_text"].apply(clean_text)
    df["word_count"] = df["clean_text"].str.split().str.len()

    if backend == "hf":
        return extract_sentiment_features_hf(df)
    return extract_sentiment_features_lexicon(df), "lexicon"


def process_and_load(backend: str = "lexicon"):
    logger.info("قراءة ملف التقييمات الخام (JSON)...")
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_reviews = json.load(f)
    logger.info("تم تحميل %d تقييم خام", len(raw_reviews))

    flat = flatten_reviews(raw_reviews)
    enriched, backend_used = extract_sentiment_features(flat, backend=backend)
    if backend_used != backend:
        logger.warning("Requested backend='%s' but actually used '%s' (fallback).", backend, backend_used)

    conn = sqlite3.connect(DB_PATH)
    enriched.drop(columns=["review_text"]).rename(
        columns={"clean_text": "review_text_clean"}
    ).to_sql("fact_reviews", conn, if_exists="replace", index=False)
    conn.close()

    logger.info("تم تحميل %d تقييم في جدول fact_reviews (backend=%s)", len(enriched), backend_used)

    # تقرير سريع: هل الـ sentiment المُستخرج من النص بيتوافق مع الـ rating الفعلي؟
    agreement = enriched.groupby("sentiment_label", observed=True)["rating"].mean().round(2)
    print("\n--- متوسط الـ rating الفعلي لكل sentiment مُستخرج من النص ---")
    print(agreement.to_string())

    return enriched


def compare_backends_on_sample(sample_size: int = 100):
    """Runs both backends on the same sample and reports where they agree/
    disagree with the actual star rating (a crude proxy for accuracy,
    since these reviews have no hand-labeled sentiment ground truth).
    Useful sanity check before trusting either backend in production."""
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_reviews = json.load(f)
    flat = flatten_reviews(raw_reviews).sample(n=min(sample_size, len(raw_reviews)), random_state=42)
    flat = flat.copy()
    flat["clean_text"] = flat["review_text"].apply(clean_text)

    def rating_to_label(r):
        return "positive" if r >= 4 else ("negative" if r <= 2 else "neutral")
    flat["rating_label"] = flat["rating"].apply(rating_to_label)

    lex = extract_sentiment_features_lexicon(flat.assign(word_count=0))
    lex_acc = (lex["sentiment_label"].astype(str) == flat["rating_label"]).mean()

    print(f"Sample size: {len(flat)}")
    print(f"lexicon backend agreement with star rating: {lex_acc:.1%}")

    # BUG FIX: this used to always print an "hf backend agreement" number
    # too, even when hf had silently fallen back to the lexicon method —
    # printing two identical numbers side by side reads as a real
    # comparison when it isn't one. Check availability *before* computing
    # anything, and print a clear "unavailable" instead of a misleading
    # second number in that case.
    if not _hf_backend_available():
        print("hf backend agreement with star rating:       unavailable "
              "(transformers/torch not installed or model unreachable — "
              "see etl/requirements-nlp.txt)")
        return

    hf, backend_used = extract_sentiment_features_hf(flat.assign(word_count=0))
    if backend_used != "hf":
        # _hf_backend_available() said yes but the actual pipeline() call
        # still failed (e.g. network unreachable at model-download time) —
        # same honesty rule applies.
        print("hf backend agreement with star rating:       unavailable "
              "(model failed to load — see the warning above)")
        return

    hf_acc = (hf["sentiment_label"].astype(str) == flat["rating_label"]).mean()
    print(f"hf backend agreement with star rating:       {hf_acc:.1%}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Process customer reviews and extract sentiment.")
    parser.add_argument("--backend", choices=["lexicon", "hf"], default="lexicon")
    parser.add_argument("--compare", action="store_true",
                         help="run both backends on a 100-review sample and report agreement with star rating, instead of loading fact_reviews")
    args = parser.parse_args()

    if args.compare:
        compare_backends_on_sample()
    else:
        process_and_load(backend=args.backend)
