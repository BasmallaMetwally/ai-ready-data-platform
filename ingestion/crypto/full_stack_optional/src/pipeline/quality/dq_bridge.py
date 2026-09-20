"""
dq_bridge.py — NEW module (added while merging the three projects)
--------------------------------------------------------------------
The crypto-market-pipeline project already had its own row-level quality
checks (pipeline/quality/checks.py: ERROR/WARN assertions that abort a
load). That's valuable but it only gives a binary pass/fail per check —
unlike the DQ system, it never produces a 0-100 score, a per-column
breakdown, or a trend history over time.

This bridge takes the crypto extractor's output (a list of Kline/OHLCV
dicts) and runs it through the SAME quality_gate.run_quality_gate() used
by the e-commerce ETL pipeline, so crypto data gets the DQ system's
scoring + `/quality/history/{name}` trend tracking for free, on top of
(not instead of) the crypto pipeline's own row-level checks.

This module is used two ways:
  - As part of unified_data_platform, where `dq/` is a sibling folder
    (locally, or mounted into the Airflow containers — see
    docker-compose.yml).
  - Standalone, if this crypto project is kept/deployed as its own repo
    (recommended in code review, since it has its own CI/test suite that
    predates the merge). In that case `dq/` does not exist at all, so the
    import below is optional: if it's missing, `run_ohlcv_quality_gate`
    logs a warning once and returns None instead of raising, so neither
    the DAG's import (Airflow's DagBag check) nor the extract task itself
    ever breaks because of this bridge.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterable
from dataclasses import asdict

import pandas as pd

logger = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
_DQ_AVAILABLE = False
for _candidate in (
    os.path.join(
        _HERE, "..", "..", "..", "..", "..", "..", "dq"
    ),  # full_stack_optional/src/pipeline/quality -> ../..(x6)/dq
    "/opt/airflow/dq",
):
    if os.path.isdir(_candidate):
        sys.path.insert(0, _candidate)
        _DQ_AVAILABLE = True
        break

if _DQ_AVAILABLE:
    from quality_gate import QualityGateResult, run_quality_gate
else:
    logger.warning(
        "dq/ package not found next to this repo — crypto DQ scoring is "
        "disabled (row-level OHLCV_SUITE checks are unaffected). This is "
        "expected if this crypto project is deployed on its own, outside "
        "unified_data_platform."
    )
    QualityGateResult = None  # type: ignore[assignment,misc]


def klines_to_dataframe(klines: Iterable) -> pd.DataFrame:
    """Converts a list of pipeline.extract.binance.Kline (or plain dicts) into
    the flat DataFrame shape the DQ engine expects."""
    rows = [asdict(k) if hasattr(k, "__dataclass_fields__") else dict(k) for k in klines]
    return pd.DataFrame(rows)


CRYPTO_OHLCV_SCHEMA = {
    "open": {"type": "numeric", "min": 0},
    "high": {"type": "numeric", "min": 0},
    "low": {"type": "numeric", "min": 0},
    "close": {"type": "numeric", "min": 0},
    "volume": {"type": "numeric", "min": 0},
}


def run_ohlcv_quality_gate(
    klines: Iterable,
    symbol: str,
    threshold: float = 80.0,
) -> QualityGateResult | None:
    """Scores one symbol's OHLCV batch with the shared DQ engine and records
    it to the same quality-history database the ETL pipeline uses, so
    `/quality/history/crypto.<SYMBOL>` shows a trend in the unified API.
    Returns None (and logs a warning) if `dq/` isn't available — see the
    module docstring."""
    if not _DQ_AVAILABLE:
        logger.warning("Skipping DQ score for %s: dq/ package not available.", symbol)
        return None

    df = klines_to_dataframe(klines)
    return run_quality_gate(
        df,
        dataset_name=f"crypto.{symbol}",
        primary_key=None,
        schema=CRYPTO_OHLCV_SCHEMA,
        threshold=threshold,
    )
