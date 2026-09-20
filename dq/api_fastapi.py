"""
Data Quality API — FastAPI version
--------------------------------------
Same logic as api.py, but as real FastAPI. This file is written to work
production-ready, but I haven't run it myself in this development
environment yet.

    pip install fastapi uvicorn[standard] python-multipart
    uvicorn api_fastapi:app --reload --port 8000

Endpoints:
    GET  /health
    POST /validate?remediate=false   (multipart file upload, not a raw body
                                       like api.py — that's the main technical
                                       difference, and it's easier to use from
                                       any HTTP client or straight from the
                                       Swagger UI at /docs)

Extra features over api.py (FastAPI gives you these for free):
- Automatic interactive docs at /docs (Swagger UI) and /redoc
- Automatic type validation on query params (Pydantic)
- Real async request handling (not manual threading)
"""
from typing import Optional
import io

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse

from validation import ValidationEngine
from scoring import QualityScorer
from auto_remediation import AutoRemediator
from config_loader import load_config, ConfigError

app = FastAPI(
    title="Data Quality Validation API",
    description="Accepts a CSV and returns a Data Quality Score + check details, with optional auto-remediation.",
    version="1.0.0",
)

try:
    CONFIG = load_config("config.json")
except ConfigError as e:
    CONFIG = {}
    print(f"Warning: could not load config.json at server startup: {e}")


def _compute(df, primary_key, schema, date_pairs, weights):
    engine = ValidationEngine(df)
    results = engine.run_all(primary_key=primary_key, schema=schema, date_pairs=date_pairs)
    scorer = QualityScorer(results, total_rows=len(df), weights=weights)
    return results, scorer.compute()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/validate")
async def validate(
    file: UploadFile = File(..., description="CSV file to validate"),
    remediate: bool = Query(False, description="If True, apply auto-remediation and return a before/after comparison"),
    dataset_name: Optional[str] = Query(None, description="Dataset name (defaults to the file name)"),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="The file must be a .csv")

    raw = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse the file as valid CSV: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="The file was read but contains no data rows")

    primary_key = CONFIG.get("primary_key")
    schema = CONFIG.get("schema", {})
    date_pairs = CONFIG.get("date_pairs", [])
    weights = CONFIG.get("scoring_weights")

    results, score_result = _compute(df, primary_key, schema, date_pairs, weights)

    response = {
        "dataset_name": dataset_name or file.filename,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "overall_score": score_result["overall_score"],
        "column_scores": score_result["column_scores"],
        "deductions": score_result["deductions"],
        "checks": {
            "missing": results.get("missing", []),
            "duplicates": results.get("duplicates", {}),
            "outliers": [{k: v for k, v in o.items() if k != "examples"} for o in results.get("outliers", [])],
            "schema": [{k: v for k, v in s.items() if k != "examples"} for s in results.get("schema", [])],
            "consistency": [{k: v for k, v in c.items() if k != "examples"} for c in results.get("consistency", [])],
        },
    }

    if remediate:
        strategies = CONFIG.get("remediation", {})
        remediator = AutoRemediator(df, results)
        remediator.remediate_missing(
            column_strategies=strategies.get("missing", {}),
            default_strategy=strategies.get("missing_default", "flag_only"),
        )
        remediator.remediate_duplicates(strategy=strategies.get("duplicates", "drop_both"), primary_key=primary_key)
        remediator.remediate_outliers(
            strategy=strategies.get("outliers", "cap"),
            exclude_columns=[primary_key] if primary_key else None,
        )
        _, after_score = _compute(remediator.df, primary_key, schema, date_pairs, weights)
        rem_summary = remediator.summary()
        response["remediation"] = {
            "score_before": score_result["overall_score"],
            "score_after": after_score["overall_score"],
            "total_actions": rem_summary.get("total_actions", 0),
            "rows_removed": rem_summary.get("rows_removed", 0),
            "skipped_columns": rem_summary.get("skipped_columns", []),
        }

    return JSONResponse(content=response)
