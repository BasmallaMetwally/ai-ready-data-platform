"""
main.py — Unified Data Platform API
-------------------------------------
This merges the three separate projects' APIs into ONE FastAPI app:

  1. dq_v2/api_fastapi.py         -> mounted under /dq  (upload a CSV, get a DQ score)
  2. ecommerce_project/api/main.py -> mounted at root     (forecast, segments, anomalies, recs)
  3. NEW endpoints (didn't exist in any of the three originals):
       - POST /pipeline/run           run the full ETL pipeline (extract -> transform ->
                                       validate -> DQ gate -> load) on demand, over HTTP
       - GET  /quality/latest         latest DQ-gate score per warehouse table
       - GET  /quality/history/{name} DQ score trend over time for a given table/dataset
                                       (reuses dq's HistoryTracker, same idea the DQ system
                                       used for a single CSV, now applied to every table the
                                       ETL pipeline touches)

Run:
    pip install -r requirements.txt
    uvicorn main:app --reload --port 8000
Then open http://localhost:8000/docs
"""
import os
import sys
import io
import json
import logging
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)                                   # api/services.py, train_and_save_models.py
sys.path.insert(0, os.path.join(HERE, "..", "dq"))          # dq system (flat modules)
sys.path.insert(0, os.path.join(HERE, "..", "etl"))         # etl pipeline
sys.path.insert(0, os.path.join(HERE, "..", "ml"))          # ml modules (used by train script)

import services  # ecommerce analytics business logic  (noqa: E402)

from validation import ValidationEngine  # dq system (noqa: E402)
from scoring import QualityScorer
from auto_remediation import AutoRemediator
from config_loader import load_config, ConfigError
from history import HistoryTracker
from quality_gate import DEFAULT_HISTORY_DB

logger = logging.getLogger("unified_api")

app = FastAPI(
    title="Unified Data Platform API",
    description=(
        "دمج نظام جودة البيانات (DQ) + منصة تحليلات التجارة الإلكترونية (ETL + ML) "
        "في API واحد، بالإضافة لنقاط نهاية جديدة لتشغيل الـ pipeline ومتابعة جودة "
        "البيانات عبر الزمن."
    ),
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

try:
    DQ_CONFIG = load_config(os.path.join(HERE, "..", "dq", "config.json"))
except ConfigError as e:
    DQ_CONFIG = {}
    logger.warning("Could not load dq/config.json at startup: %s", e)


# =================================================================
# System
# =================================================================
class HealthResponse(BaseModel):
    status: str
    database: bool
    models_loaded: bool
    dq_config_loaded: bool


@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    h = services.health_check()
    h["dq_config_loaded"] = bool(DQ_CONFIG)
    return h


# =================================================================
# 1) DQ endpoints (from dq_v2/api_fastapi.py) — mounted under /dq
# =================================================================
def _compute_dq(df, primary_key, schema, date_pairs, weights):
    engine = ValidationEngine(df)
    results = engine.run_all(primary_key=primary_key, schema=schema, date_pairs=date_pairs)
    scorer = QualityScorer(results, total_rows=len(df), weights=weights)
    return results, scorer.compute()


@app.post("/dq/validate", tags=["Data Quality"])
async def dq_validate(
    file: UploadFile = File(..., description="CSV file to check"),
    remediate: bool = Query(False, description="Apply auto-remediation and return before/after"),
    dataset_name: Optional[str] = Query(None),
):
    """Upload any CSV and get back a Data Quality Score + check breakdown."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="الملف لازم يكون CSV")

    raw = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"تعذر قراءة الملف كـ CSV صالح: {e}")
    if df.empty:
        raise HTTPException(status_code=400, detail="الملف اتقرا لكنه من غير صفوف بيانات")

    primary_key = DQ_CONFIG.get("primary_key")
    schema = DQ_CONFIG.get("schema", {})
    date_pairs = DQ_CONFIG.get("date_pairs", [])
    weights = DQ_CONFIG.get("scoring_weights")

    results, score_result = _compute_dq(df, primary_key, schema, date_pairs, weights)
    name = dataset_name or file.filename

    response = {
        "dataset_name": name,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "overall_score": score_result["overall_score"],
        "column_scores": score_result["column_scores"],
        "deductions": score_result["deductions"],
    }

    if remediate:
        strategies = DQ_CONFIG.get("remediation", {})
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
        cleaned_df = remediator.df
        cleaned_results, cleaned_score = _compute_dq(cleaned_df, primary_key, schema, date_pairs, weights)
        # BUG FIX (found while wiring this into the unified API): get_audit_trail()
        # returns a pandas DataFrame with numpy int64/float64 cells, which FastAPI's
        # JSON encoder cannot serialize on its own ("numpy.int64 is not iterable").
        # Converting through pandas' own JSON round-trip is the simplest safe fix.
        audit_df = remediator.get_audit_trail()
        audit_records = json.loads(audit_df.to_json(orient="records")) if not audit_df.empty else []
        response["after_remediation"] = {
            "overall_score": cleaned_score["overall_score"],
            "column_scores": cleaned_score["column_scores"],
            "audit_trail": audit_records,
        }

    # every /dq/validate call is also recorded to history, so trends show up
    # in /quality/history/{name} regardless of whether it came from the ETL
    # pipeline or an ad-hoc upload.
    try:
        HistoryTracker(db_path=DEFAULT_HISTORY_DB).save_run(
            name, len(df), score_result["overall_score"], score_result["column_scores"]
        )
    except Exception:
        logger.exception("Could not save DQ history for ad-hoc upload '%s'", name)

    return response


# =================================================================
# 2) Quality tracking — NEW, ties the DQ system's history tracker to every
#    table the ETL pipeline loads, not just a single uploaded file.
# =================================================================
@app.get("/quality/history/{dataset_name}", tags=["Data Quality"])
def quality_history(dataset_name: str, limit: int = Query(50, ge=1, le=500)):
    tracker = HistoryTracker(db_path=DEFAULT_HISTORY_DB)
    rows = tracker.get_history(dataset_name=dataset_name, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"لا يوجد تاريخ جودة بيانات مسجل لـ '{dataset_name}'")
    return rows


@app.get("/quality/latest", tags=["Data Quality"])
def quality_latest():
    """Latest DQ-gate score for each ETL star-schema table (dim_customer, dim_product, fact_sales)."""
    tracker = HistoryTracker(db_path=DEFAULT_HISTORY_DB)
    out = {}
    for table in ("etl.dim_customer", "etl.dim_product", "etl.fact_sales"):
        rows = tracker.get_history(dataset_name=table, limit=1)
        if rows:
            out[table] = rows[0]
    if not out:
        raise HTTPException(status_code=404, detail="لسه مفيش أي تشغيلة للـ pipeline لتسجيل جودة البيانات")
    return out


# =================================================================
# 3) Pipeline control — NEW, run the ETL pipeline over HTTP
# =================================================================
class PipelineRunResponse(BaseModel):
    status: str
    detail: str


@app.post("/pipeline/run", response_model=PipelineRunResponse, tags=["Pipeline"])
def run_pipeline_endpoint(
    dq_threshold: float = Query(80.0, ge=0, le=100),
    skip_dq_gate: bool = Query(False),
):
    """Runs Extract -> Transform -> Validate -> DQ Gate -> Load synchronously
    and reports success/failure. Intended for demos/small datasets; for a
    real production schedule use orchestration/simple_scheduler.py or
    orchestration/airflow_dag.py instead of calling this on every request.
    """
    from pipeline import run_pipeline, PipelineError

    try:
        run_pipeline(dq_threshold=dq_threshold, skip_dq_gate=skip_dq_gate)
    except PipelineError as e:
        raise HTTPException(status_code=422, detail=f"فشل الـ pipeline: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ غير متوقع: {e}")

    # invalidate the in-memory model/data caches in services.py so subsequent
    # /forecast, /segments, etc. reflect the freshly loaded data on next call
    services._forecast_bundle = None
    services._segmentation_lookup = None
    services._anomaly_lookup = None
    services._recommendation_bundle = None

    return {"status": "ok", "detail": "الـ pipeline اشتغل بنجاح (extract -> transform -> validate -> dq gate -> load)"}


# =================================================================
# 4) E-commerce analytics endpoints (from ecommerce_project/api/main.py)
# =================================================================
class ForecastPoint(BaseModel):
    date: str
    predicted_revenue: float


class CustomerSegment(BaseModel):
    customer_id: int
    segment: str
    recency_days: int
    frequency: int
    monetary: float


class SegmentSummary(BaseModel):
    segment: str
    n_customers: int
    avg_recency: float
    avg_frequency: float
    avg_monetary: float


class AnomalyDay(BaseModel):
    date: str
    revenue: float
    n_orders: int
    anomaly_score: float


class ProductRecommendation(BaseModel):
    product_id: int
    similarity_score: Optional[float] = None
    score: Optional[float] = None


@app.get("/forecast", response_model=List[ForecastPoint], tags=["Forecasting"])
def get_forecast(days: int = Query(7, ge=1, le=30)):
    return services.forecast_next_days(days)


@app.get("/customers/{customer_id}/segment", response_model=CustomerSegment, tags=["Customers"])
def get_customer_segment(customer_id: int):
    result = services.get_customer_segment(customer_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"العميل {customer_id} غير موجود")
    return result


@app.get("/segments/summary", response_model=List[SegmentSummary], tags=["Customers"])
def get_segments_summary():
    return services.list_segments_summary()


@app.get("/sales/anomalies", response_model=List[AnomalyDay], tags=["Analytics"])
def get_anomalies(limit: int = Query(10, ge=1, le=100)):
    return services.get_recent_anomalies(limit)


@app.get("/products/{product_id}/similar", response_model=List[ProductRecommendation], tags=["Recommendations"])
def get_similar_products(product_id: int, top_n: int = Query(5, ge=1, le=20)):
    results = services.recommend_similar_products(product_id, top_n)
    if not results:
        raise HTTPException(status_code=404, detail=f"مفيش بيانات كافية عن المنتج {product_id}")
    return results


@app.get("/customers/{customer_id}/recommendations", response_model=List[ProductRecommendation], tags=["Recommendations"])
def get_customer_recommendations(customer_id: int, top_n: int = Query(5, ge=1, le=20)):
    results = services.recommend_for_customer(customer_id, top_n)
    if not results:
        raise HTTPException(status_code=404, detail=f"مفيش تاريخ شراء كافي للعميل {customer_id}")
    return results


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
