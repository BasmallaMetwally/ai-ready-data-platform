"""
Data Quality API (stdlib-only)
---------------------------------
A real, working API built with http.server (built into Python) instead of
FastAPI, so it runs immediately with zero external dependencies — handy if
you want to spin it up quickly or on a machine where installing new
libraries isn't an option. This isn't a better technical choice than
FastAPI, it's a "zero dependency" option alongside it. If you have
FastAPI/uvicorn installed, use `api_fastapi.py` next to it for automatic
docs and stronger request validation (same logic, real production-ready
FastAPI).

Endpoints:
    GET  /health                         -> {"status": "ok"}
    POST /validate?name=X&remediate=1    -> body: raw CSV bytes (Content-Type: text/csv)
                                             returns JSON with the score + all check results
                                             (+ remediation info if remediate=1)

Run it:
    python3 api.py --port 8000

Quick test (no extra library needed):
    curl -X POST "http://localhost:8000/validate?name=test" \\
         -H "Content-Type: text/csv" \\
         --data-binary @customers_orders.csv
"""
import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import io

import pandas as pd

from validation import ValidationEngine
from scoring import QualityScorer
from auto_remediation import AutoRemediator
from config_loader import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("dq_system.api")

CONFIG = load_config("config.json")


def _compute(df, primary_key, schema, date_pairs, weights):
    engine = ValidationEngine(df)
    results = engine.run_all(primary_key=primary_key, schema=schema, date_pairs=date_pairs)
    scorer = QualityScorer(results, total_rows=len(df), weights=weights)
    return results, scorer.compute()


def _run_validation_json(df, remediate=False):
    primary_key = CONFIG.get("primary_key")
    schema = CONFIG.get("schema", {})
    date_pairs = CONFIG.get("date_pairs", [])
    weights = CONFIG.get("scoring_weights")

    results, score_result = _compute(df, primary_key, schema, date_pairs, weights)

    response = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "overall_score": score_result["overall_score"],
        "column_scores": score_result["column_scores"],
        "deductions": score_result["deductions"],
        "checks": {
            "missing": results.get("missing", []),
            "duplicates": results.get("duplicates", {}),
            "outliers": [
                {k: v for k, v in o.items() if k != "examples"}  # drop row examples to keep the API response readable
                for o in results.get("outliers", [])
            ],
            "schema": [
                {k: v for k, v in s.items() if k != "examples"}
                for s in results.get("schema", [])
            ],
            "consistency": [
                {k: v for k, v in c.items() if k != "examples"}
                for c in results.get("consistency", [])
            ],
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
        after_results, after_score = _compute(remediator.df, primary_key, schema, date_pairs, weights)
        rem_summary = remediator.summary()
        response["remediation"] = {
            "score_before": score_result["overall_score"],
            "score_after": after_score["overall_score"],
            "total_actions": rem_summary.get("total_actions", 0),
            "rows_removed": rem_summary.get("rows_removed", 0),
            "skipped_columns": rem_summary.get("skipped_columns", []),
        }

    return response


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logger.info("%s - %s" % (self.address_string(), format % args))

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False, default=str, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "not found", "available_endpoints": ["GET /health", "POST /validate"]})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/validate":
            self._send_json(404, {"error": "not found", "available_endpoints": ["GET /health", "POST /validate"]})
            return

        qs = parse_qs(parsed.query)
        remediate = qs.get("remediate", ["0"])[0] in ("1", "true", "True")
        name = qs.get("name", ["uploaded"])[0]

        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            self._send_json(400, {"error": "request body is empty — send CSV bytes in the body"})
            return

        raw = self.rfile.read(length)
        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception as e:
            self._send_json(400, {"error": f"Could not parse the file as valid CSV: {e}"})
            return

        if df.empty:
            self._send_json(400, {"error": "The CSV was read but contains no data rows"})
            return

        try:
            result = _run_validation_json(df, remediate=remediate)
            result["dataset_name"] = name
            self._send_json(200, result)
        except Exception as e:
            logger.exception("Error while processing request")
            self._send_json(500, {"error": f"Internal error while processing: {e}"})


def run_server(port=8000):
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    logger.info(f"Data Quality API running at http://0.0.0.0:{port}")
    logger.info("Try: curl http://localhost:%d/health" % port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality API server")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    run_server(args.port)
