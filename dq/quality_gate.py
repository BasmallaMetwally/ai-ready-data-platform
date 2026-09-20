"""
quality_gate.py
----------------
NEW MODULE (added while merging the three original projects).

The original dq_v2 system was a standalone CLI: you pointed it at a CSV
file and it produced a report. Nothing else in the codebase actually
*used* it as a quality gate inside a pipeline.

This module turns the DQ engine into an importable library function so
any pipeline (the e-commerce ETL, the crypto ingestion, or anything
else) can call `run_quality_gate(df, ...)` on an in-memory DataFrame
*before* loading it into a warehouse, and block the load if the score
is too low. It reuses the exact same ValidationEngine / QualityScorer /
HistoryTracker classes from the original DQ system - no logic is
duplicated or reimplemented.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from validation import ValidationEngine
from scoring import QualityScorer
from history import HistoryTracker

logger = logging.getLogger("dq.quality_gate")

DEFAULT_HISTORY_DB = os.path.join(os.path.dirname(__file__), "quality_history.db")


class QualityGateFailure(Exception):
    """Raised when a dataset's DQ score falls below the configured threshold."""

    def __init__(self, dataset_name: str, score: float, threshold: float, result: "QualityGateResult"):
        super().__init__(
            f"Quality gate failed for '{dataset_name}': score {score} < threshold {threshold}"
        )
        self.dataset_name = dataset_name
        self.score = score
        self.threshold = threshold
        self.result = result


@dataclass
class QualityGateResult:
    dataset_name: str
    overall_score: float
    column_scores: dict
    deductions: list
    passed: bool
    threshold: float
    total_rows: int
    run_id: Optional[int] = field(default=None)


def run_quality_gate(
    df,
    dataset_name: str,
    *,
    primary_key: Optional[str] = None,
    schema: Optional[dict] = None,
    date_pairs: Optional[list] = None,
    weights: Optional[dict] = None,
    threshold: float = 80.0,
    history_db: str = DEFAULT_HISTORY_DB,
    record_history: bool = True,
    raise_on_fail: bool = False,
) -> QualityGateResult:
    """Run the DQ engine on an in-memory DataFrame and return a pass/fail verdict.

    This is the integration point that lets *any* pipeline (ETL, crypto
    ingestion, etc.) reuse the DQ engine as a gate rather than a
    stand-alone report generator.
    """
    schema = schema or {}
    date_pairs = date_pairs or []

    if df is None or len(df) == 0:
        logger.warning("Quality gate for '%s': dataset is empty, scoring as 0.", dataset_name)
        result = QualityGateResult(
            dataset_name=dataset_name, overall_score=0.0, column_scores={},
            deductions=[{"reason": "empty dataset", "points": 100, "column": None}],
            passed=False, threshold=threshold, total_rows=0,
        )
    else:
        engine = ValidationEngine(df)
        checks = engine.run_all(primary_key=primary_key, schema=schema, date_pairs=date_pairs)
        scorer = QualityScorer(checks, total_rows=len(df), weights=weights)
        scored = scorer.compute()

        passed = scored["overall_score"] >= threshold
        result = QualityGateResult(
            dataset_name=dataset_name,
            overall_score=scored["overall_score"],
            column_scores=scored["column_scores"],
            deductions=scored["deductions"],
            passed=passed,
            threshold=threshold,
            total_rows=len(df),
        )

    if record_history:
        try:
            tracker = HistoryTracker(db_path=history_db)
            result.run_id = tracker.save_run(
                dataset_name, result.total_rows, result.overall_score, result.column_scores
            )
        except Exception:
            logger.exception("Could not record quality-gate history for '%s' (continuing).", dataset_name)

    level = logging.INFO if result.passed else logging.WARNING
    logger.log(
        level,
        "Quality gate '%s': score=%.1f threshold=%.1f -> %s",
        dataset_name, result.overall_score, threshold, "PASS" if result.passed else "FAIL",
    )

    if raise_on_fail and not result.passed:
        raise QualityGateFailure(dataset_name, result.overall_score, threshold, result)

    return result
