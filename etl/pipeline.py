"""
pipeline.py
-----------
منسق الـ ETL Pipeline الكامل: Extract -> Validate -> Transform -> Load

مميزات النسخة دي:
  - Logging مركزي (console + ملف مع Rotation)
  - Retry logic لخطوة الـ Extract (بعض الأعطال بتكون مؤقتة زي انقطاع الشبكة)
  - Data Validation بعد التحويل مباشرة وقبل التحميل في القاعدة
  - Error handling واضح لكل خطوة مع رسائل مفيدة للـ troubleshooting
"""

import logging
import os
import sys
import time

from extract import extract_all
from transform import transform_all
from load import load_all
from validate import validate_all
from logging_config import setup_logging
from exceptions import PipelineError, ExtractionError, DataValidationError, LoadError

# NEW (merge): reuse the DQ system as an importable quality gate instead of
# it only being a stand-alone CLI. dq/ lives as a sibling package next to etl/.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dq"))
from quality_gate import run_quality_gate, QualityGateFailure  # noqa: E402

logger = logging.getLogger("etl.pipeline")

# Minimal DQ schema/threshold config per star-schema table. Kept here (rather
# than dq/config.json, which is tuned for the customers_orders sample file)
# because each table has its own primary key and expected ranges.
DQ_TABLE_CONFIGS = {
    "dim_customer": dict(primary_key="customer_id", schema={"age": {"type": "numeric", "min": 0, "max": 120}}),
    "dim_product": dict(primary_key="product_id", schema={"unit_price": {"type": "numeric", "min": 0}}),
    "fact_sales": dict(primary_key=None, schema={"quantity": {"type": "numeric", "min": 1}}),
}


def with_retry(fn, *, retries: int = 3, delay_seconds: float = 2.0, step_name: str = "step"):
    """يعيد محاولة تنفيذ fn عند الفشل، بعدد محاولات محدود وتأخير بينهم."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            logger.warning("فشلت %s (محاولة %d/%d): %s", step_name, attempt, retries, e)
            if attempt < retries:
                time.sleep(delay_seconds)
    raise ExtractionError(f"فشلت خطوة '{step_name}' بعد {retries} محاولات") from last_exc


def run_dq_gate(transformed: dict, threshold: float = 80.0, fail_hard: bool = True) -> dict:
    """NEW step (merge): runs the DQ engine on every star-schema table right
    before Load, and blocks the load if any table's score falls below
    `threshold`. Every run is also recorded in dq/quality_history.db so
    trends across pipeline runs can be tracked, exactly like the original
    DQ system tracked trends for a single file.
    """
    gate_results = {}
    failures = []
    for table_name, cfg in DQ_TABLE_CONFIGS.items():
        df = transformed.get(table_name)
        if df is None:
            continue
        result = run_quality_gate(
            df, dataset_name=f"etl.{table_name}",
            primary_key=cfg["primary_key"], schema=cfg["schema"],
            threshold=threshold, raise_on_fail=False,
        )
        gate_results[table_name] = result
        if not result.passed:
            failures.append(result)

    if failures and fail_hard:
        names = ", ".join(f"{r.dataset_name} ({r.overall_score}/100)" for r in failures)
        raise DataValidationError(
            f"DQ quality gate blocked the load — تحت الحد الأدنى: {names}",
            failed_checks=[r.dataset_name for r in failures],
        )

    return gate_results


def run_pipeline(
    skip_validation: bool = False,
    dq_threshold: float = 80.0,
    skip_dq_gate: bool = False,
    target: str = "sqlite",
    database_url: str | None = None,
):
    setup_logging()
    start = time.time()
    logger.info("========== بدء ETL Pipeline ==========")

    try:
        logger.info("[1/5] Extract...")
        raw = with_retry(extract_all, retries=3, step_name="extract_all")
    except ExtractionError:
        logger.exception("فشل نهائي في مرحلة Extract — تم إيقاف الـ pipeline.")
        raise

    try:
        logger.info("[2/5] Transform...")
        transformed = transform_all(raw)
    except Exception as e:
        logger.exception("فشل في مرحلة Transform: %s", e)
        raise PipelineError("Transform failed") from e

    if not skip_validation:
        try:
            logger.info("[3/5] Validate (expectations)...")
            validate_all(transformed)
        except DataValidationError as e:
            logger.error("توقف الـ pipeline بسبب فشل التحقق من جودة البيانات: %s", e)
            logger.error("الاختبارات الفاشلة: %s", e.failed_checks)
            raise
    else:
        logger.warning("[3/5] Validate... تم تخطيها (skip_validation=True)")

    if not skip_dq_gate:
        try:
            logger.info("[4/5] Data Quality Gate (DQ score per table)...")
            gate_results = run_dq_gate(transformed, threshold=dq_threshold, fail_hard=True)
            for name, r in gate_results.items():
                logger.info("  DQ[%s] = %.1f/100 (%s)", name, r.overall_score, "PASS" if r.passed else "FAIL")
        except DataValidationError as e:
            logger.error("توقف الـ pipeline: %s", e)
            raise
    else:
        logger.warning("[4/5] DQ Gate... تم تخطيها (skip_dq_gate=True)")

    try:
        logger.info("[5/5] Load...")
        load_all(transformed, target=target, database_url=database_url)
    except Exception as e:
        logger.exception("فشل في مرحلة Load: %s", e)
        raise LoadError("Load failed") from e

    elapsed = time.time() - start
    logger.info("========== انتهى الـ Pipeline بنجاح في %.2f ثانية ==========", elapsed)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the e-commerce ETL pipeline.")
    parser.add_argument("--target", choices=("sqlite", "postgres", "mysql"), default="sqlite")
    parser.add_argument("--database-url", help="Required for postgres/mysql; defaults to DATABASE_URL.")
    parser.add_argument("--dq-threshold", type=float, default=80.0)
    args = parser.parse_args()
    try:
        run_pipeline(target=args.target, database_url=args.database_url, dq_threshold=args.dq_threshold)
    except PipelineError:
        logger.critical("توقف الـ Pipeline بسبب خطأ غير قابل للتعافي. راجع logs/etl.log للتفاصيل.")
        raise
