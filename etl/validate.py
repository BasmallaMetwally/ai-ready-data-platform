"""
validate.py
-----------
طبقة التحقق من جودة البيانات (Data Validation) — أسلوب مشابه لـ
Great Expectations لكن خفيف ومبني بـ Pandas بحت (عشان يشتغل من غير
مكتبات خارجية).

الفكرة: كل جدول له مجموعة "Expectations" (توقعات) — لو اتكسر
expectation حرِج (critical) الـ pipeline يوقف، ولو اتكسر expectation
غير حرج (warning) الـ pipeline يكمّل لكن يسجّل تحذير في اللوج.
"""

import logging
import pandas as pd
from dataclasses import dataclass, field
from typing import Callable, List

from exceptions import DataValidationError

logger = logging.getLogger("etl.validate")


@dataclass
class CheckResult:
    name: str
    passed: bool
    critical: bool
    detail: str = ""


@dataclass
class Expectation:
    name: str
    check_fn: Callable[[pd.DataFrame], bool]
    detail_fn: Callable[[pd.DataFrame], str] = field(default=lambda df: "")
    critical: bool = True


def not_null(column: str) -> Expectation:
    return Expectation(
        name=f"{column}: لا يحتوي على قيم فارغة",
        check_fn=lambda df: df[column].isna().sum() == 0,
        detail_fn=lambda df: f"{df[column].isna().sum()} قيمة فارغة",
        critical=True,
    )


def unique(column: str) -> Expectation:
    return Expectation(
        name=f"{column}: قيم فريدة (لا تكرار)",
        check_fn=lambda df: df[column].is_unique,
        detail_fn=lambda df: f"{df[column].duplicated().sum()} صف مكرر",
        critical=True,
    )


def in_range(column: str, min_val=None, max_val=None) -> Expectation:
    def _check(df):
        s = df[column]
        ok = True
        if min_val is not None:
            ok &= (s >= min_val).all()
        if max_val is not None:
            ok &= (s <= max_val).all()
        return ok

    def _detail(df):
        s = df[column]
        bad = 0
        if min_val is not None:
            bad += (s < min_val).sum()
        if max_val is not None:
            bad += (s > max_val).sum()
        return f"{bad} قيمة خارج النطاق [{min_val}, {max_val}]"

    return Expectation(
        name=f"{column}: داخل النطاق المسموح",
        check_fn=_check, detail_fn=_detail, critical=True,
    )


def allowed_values(column: str, values: set, critical: bool = False) -> Expectation:
    return Expectation(
        name=f"{column}: قيم ضمن المسموح {sorted(values)}",
        check_fn=lambda df: set(df[column].dropna().unique()).issubset(values),
        detail_fn=lambda df: f"قيم غير متوقعة: {set(df[column].dropna().unique()) - values}",
        critical=critical,
    )


def referential_integrity(column: str, ref_df: pd.DataFrame, ref_column: str) -> Expectation:
    """يتأكد إن كل قيمة في column موجودة فعلاً في جدول آخر (Foreign Key check)."""
    return Expectation(
        name=f"{column}: تكامل مرجعي مع {ref_column}",
        check_fn=lambda df: df[column].isin(ref_df[ref_column]).all(),
        detail_fn=lambda df: f"{(~df[column].isin(ref_df[ref_column])).sum()} صف بدون مرجع صحيح",
        critical=True,
    )


def row_count_positive() -> Expectation:
    return Expectation(
        name="الجدول غير فارغ",
        check_fn=lambda df: len(df) > 0,
        detail_fn=lambda df: f"عدد الصفوف: {len(df)}",
        critical=True,
    )


def run_suite(df: pd.DataFrame, expectations: List[Expectation], table_name: str) -> List[CheckResult]:
    results = []
    for exp in expectations:
        try:
            passed = bool(exp.check_fn(df))
        except Exception as e:
            passed = False
            exp.detail_fn = lambda df, err=e: f"خطأ أثناء التحقق: {err}"

        detail = exp.detail_fn(df) if not passed else ""
        results.append(CheckResult(exp.name, passed, exp.critical, detail))

        icon = "✅" if passed else ("🛑" if exp.critical else "⚠️")
        msg = f"{icon} [{table_name}] {exp.name}"
        if not passed:
            msg += f" -> {detail}"
        if passed:
            logger.debug(msg)
        elif exp.critical:
            logger.error(msg)
        else:
            logger.warning(msg)

    return results


def validate_table(df: pd.DataFrame, expectations: List[Expectation], table_name: str) -> List[CheckResult]:
    """يشغّل كل الـ expectations، ويرفع DataValidationError لو فيه فشل حرِج."""
    results = run_suite(df, expectations, table_name)
    critical_failures = [r for r in results if r.critical and not r.passed]

    if critical_failures:
        names = [r.name for r in critical_failures]
        raise DataValidationError(
            f"فشل {len(critical_failures)} اختبار حرِج في جدول '{table_name}': {names}",
            failed_checks=names,
        )

    return results


def validate_all(tables: dict) -> dict:
    """يشغّل كل التحققات على جداول الـ Star Schema المُحوّلة."""
    logger.info("بدء التحقق من جودة البيانات (Data Validation)...")

    report = {}

    report["dim_customer"] = validate_table(
        tables["dim_customer"],
        [
            row_count_positive(),
            not_null("customer_id"),
            unique("customer_id"),
            not_null("name"),
            in_range("age", min_val=0, max_val=120),
        ],
        "dim_customer",
    )

    report["dim_product"] = validate_table(
        tables["dim_product"],
        [
            row_count_positive(),
            not_null("product_id"),
            unique("product_id"),
            in_range("unit_price", min_val=0),
            in_range("margin_pct", min_val=-1, max_val=1),
        ],
        "dim_product",
    )

    report["fact_sales"] = validate_table(
        tables["fact_sales"],
        [
            row_count_positive(),
            not_null("customer_id"),
            not_null("product_id"),
            in_range("quantity", min_val=1),
            in_range("total_amount", min_val=0),
            allowed_values("status", {"completed", "cancelled", "returned", "unknown"}, critical=True),
            referential_integrity("customer_id", tables["dim_customer"], "customer_id"),
            referential_integrity("product_id", tables["dim_product"], "product_id"),
        ],
        "fact_sales",
    )

    total_checks = sum(len(v) for v in report.values())
    total_failed = sum(1 for v in report.values() for r in v if not r.passed)
    logger.info("انتهى التحقق: %d/%d اختبار ناجح", total_checks - total_failed, total_checks)

    return report
