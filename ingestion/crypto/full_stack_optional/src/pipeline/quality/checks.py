"""A small declarative data-quality framework for the bronze layer.

dbt tests guard the warehouse, but by then bad data has already been persisted.
These checks run *before* the load, so a corrupted extract fails the Airflow
task instead of silently poisoning the marts.

Severities:
    ERROR — abort the run.
    WARN  — record the failure, let the pipeline continue.
"""

from __future__ import annotations

import datetime as dt
import enum
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

Rows = Sequence[dict[str, Any]]


class Severity(enum.StrEnum):
    ERROR = "error"
    WARN = "warn"


class DataQualityError(AssertionError):
    """Raised when at least one ERROR-severity check fails."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    severity: Severity
    failed_rows: int
    detail: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else f"FAIL ({self.severity})"
        suffix = f" — {self.detail}" if self.detail else ""
        return f"[{status}] {self.name}{suffix}"


@dataclass(frozen=True, slots=True)
class Check:
    """A named predicate applied row-wise to an extract."""

    name: str
    predicate: Callable[[dict[str, Any]], bool]
    severity: Severity = Severity.ERROR
    description: str = ""

    def run(self, rows: Rows) -> CheckResult:
        failures = [r for r in rows if not self._safe(r)]
        sample = ""
        if failures:
            first = failures[0]
            sample = (
                f"{len(failures)}/{len(rows)} rows failed; "
                f"first offender: symbol={first.get('symbol')} open_time={first.get('open_time')}"
            )
        return CheckResult(
            name=self.name,
            passed=not failures,
            severity=self.severity,
            failed_rows=len(failures),
            detail=sample or self.description,
        )

    def _safe(self, row: dict[str, Any]) -> bool:
        try:
            return self.predicate(row)
        except (TypeError, ValueError, KeyError):
            return False


@dataclass
class Suite:
    """An ordered collection of checks plus dataset-level assertions."""

    name: str
    checks: list[Check] = field(default_factory=list)

    def run(self, rows: Rows) -> list[CheckResult]:
        results = [c.run(rows) for c in self.checks]
        results.extend(self._dataset_checks(rows))
        for result in results:
            (log.error if not result.passed and result.severity is Severity.ERROR else log.info)(
                "%s :: %s", self.name, result
            )
        errors = [r for r in results if not r.passed and r.severity is Severity.ERROR]
        if errors:
            raise DataQualityError(
                f"{len(errors)} blocking data-quality failure(s) in suite '{self.name}': "
                + "; ".join(r.name for r in errors)
            )
        return results

    def _dataset_checks(self, rows: Rows) -> list[CheckResult]:
        results: list[CheckResult] = []

        results.append(
            CheckResult(
                name="dataset_not_empty",
                passed=bool(rows),
                severity=Severity.ERROR,
                failed_rows=0 if rows else 1,
                detail="extract returned zero rows",
            )
        )

        seen: set[tuple] = set()
        duplicates = 0
        for row in rows:
            key = (row.get("symbol"), row.get("interval"), row.get("open_time"))
            if key in seen:
                duplicates += 1
            seen.add(key)
        results.append(
            CheckResult(
                name="primary_key_is_unique",
                passed=duplicates == 0,
                severity=Severity.ERROR,
                failed_rows=duplicates,
                detail=f"{duplicates} duplicate (symbol, interval, open_time) keys",
            )
        )
        return results


# --------------------------------------------------------------------- suites
def _is_positive(value: Any) -> bool:
    return value is not None and float(value) > 0


OHLCV_SUITE = Suite(
    name="bronze.binance_klines",
    checks=[
        Check(
            "prices_are_positive",
            lambda r: all(_is_positive(r[c]) for c in ("open", "high", "low", "close")),
            description="OHLC values must be greater than zero",
        ),
        Check(
            "high_is_the_maximum",
            lambda r: r["high"] >= max(r["open"], r["close"], r["low"]),
            description="high must be >= open, close and low",
        ),
        Check(
            "low_is_the_minimum",
            lambda r: r["low"] <= min(r["open"], r["close"], r["high"]),
            description="low must be <= open, close and high",
        ),
        Check(
            "volume_is_not_negative",
            lambda r: float(r["volume"]) >= 0,
            description="traded volume cannot be negative",
        ),
        Check(
            "close_time_follows_open_time",
            lambda r: r["close_time"] > r["open_time"],
            description="candle must have positive duration",
        ),
        Check(
            "candle_is_already_closed",
            lambda r: r["close_time"] <= dt.datetime.now(tz=dt.UTC),
            description="partial candles must not reach the warehouse",
        ),
        Check(
            "trade_count_is_plausible",
            lambda r: int(r["trade_count"]) >= 0,
            severity=Severity.WARN,
            description="zero-trade candles happen on illiquid pairs but are worth flagging",
        ),
        Check(
            "taker_volume_within_total",
            lambda r: float(r["taker_buy_base_volume"]) <= float(r["volume"]) * 1.0001,
            severity=Severity.WARN,
            description="taker buy volume is a subset of total volume",
        ),
    ],
)


def check_no_date_gaps(rows: Rows, expected_days: int, tolerance: int = 1) -> CheckResult:
    """Completeness check: a daily feed should deliver roughly one bar per day.

    Crypto markets never close, so unlike equities a missing day is a genuine
    defect rather than a weekend.
    """
    distinct_days = {r["open_time"].date() for r in rows if r.get("open_time")}
    missing = expected_days - len(distinct_days)
    return CheckResult(
        name="no_missing_days",
        passed=missing <= tolerance,
        severity=Severity.ERROR,
        failed_rows=max(missing, 0),
        detail=f"expected ~{expected_days} days, received {len(distinct_days)}",
    )


# ---------------------------------------------------------- anomaly detection
"""
Everything above is a *deterministic* check: a rule that is either satisfied or
not. Those catch corrupt rows but are blind to the most common real-world
failure, where every row is individually valid and the dataset as a whole is
wrong — an upstream that starts returning half its usual volume, or a symbol
that quietly stops trading. The checks below compare against recent history.
"""


@dataclass(frozen=True, slots=True)
class Baseline:
    """Summary statistics from a trailing window, used as the expectation."""

    mean: float
    stddev: float
    sample_size: int

    def z_score(self, value: float) -> float:
        if self.stddev == 0:
            return 0.0 if value == self.mean else float("inf")
        return (value - self.mean) / self.stddev


def compute_baseline(history: Sequence[float]) -> Baseline | None:
    """Mean and sample standard deviation of a trailing window.

    Returns None below 7 observations: a z-score over a handful of points is
    noise dressed up as a metric, and firing on it trains people to ignore it.
    """
    values = [float(v) for v in history if v is not None]
    if len(values) < 7:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return Baseline(mean=mean, stddev=variance**0.5, sample_size=len(values))


def check_row_count_anomaly(
    observed: int,
    history: Sequence[float],
    z_threshold: float = 3.0,
    severity: Severity = Severity.WARN,
) -> CheckResult:
    """Flag an extract that is unusually large or small versus recent runs."""
    baseline = compute_baseline(history)
    if baseline is None:
        return CheckResult(
            name="row_count_within_expected_range",
            passed=True,
            severity=severity,
            failed_rows=0,
            detail=f"insufficient history ({len(history)} runs); check skipped",
        )

    z = baseline.z_score(observed)
    return CheckResult(
        name="row_count_within_expected_range",
        passed=abs(z) <= z_threshold,
        severity=severity,
        failed_rows=0 if abs(z) <= z_threshold else 1,
        detail=(
            f"observed {observed} rows, baseline {baseline.mean:.1f} "
            f"± {baseline.stddev:.1f} over {baseline.sample_size} runs (z={z:+.2f})"
        ),
    )


def check_volume_anomaly(
    rows: Rows,
    history: Sequence[float],
    z_threshold: float = 4.0,
) -> CheckResult:
    """Flag a collapse or explosion in traded volume.

    Threshold is looser than for row counts because genuine market events do move
    volume by several standard deviations — this is a "look at it" signal, not a
    "stop the pipeline" one, which is why it is WARN severity.
    """
    observed = sum(float(r.get("quote_volume") or 0) for r in rows)
    baseline = compute_baseline(history)
    if baseline is None:
        return CheckResult(
            name="volume_within_expected_range",
            passed=True,
            severity=Severity.WARN,
            failed_rows=0,
            detail="insufficient history; check skipped",
        )

    z = baseline.z_score(observed)
    return CheckResult(
        name="volume_within_expected_range",
        passed=abs(z) <= z_threshold,
        severity=Severity.WARN,
        failed_rows=0 if abs(z) <= z_threshold else 1,
        detail=f"observed ${observed:,.0f} quote volume (z={z:+.2f})",
    )


def fetch_recent_row_counts(dag_id: str, task_id: str, limit: int = 30) -> list[float]:
    """Read trailing row counts from the audit table to build a baseline.

    Returns an empty list on any error, which makes the anomaly checks degrade
    to "skipped" rather than failing the run — the same principle as the audit
    writer: observability must not break what it observes.
    """
    from pipeline.load.warehouse import connection

    try:
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT row_count FROM raw.pipeline_audit
                WHERE dag_id = %s AND task_id = %s
                  AND status = 'success' AND row_count IS NOT NULL
                ORDER BY logical_date DESC LIMIT %s
                """,
                (dag_id, task_id, limit),
            )
            return [float(r[0]) for r in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001
        log.warning("could not load baseline history: %s", exc)
        return []
