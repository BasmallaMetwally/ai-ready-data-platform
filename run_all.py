"""
run_all.py — Unified Data Platform, single entry point
==========================================================
NEW (added while merging the three projects). None of the three original
projects had one command that runs the whole thing end to end. This does:

    1. DQ check   -> run the standalone DQ system against a sample CSV
                     (dq/customers_orders.csv) and print a report,
                     exactly like the original dq_v2/main.py did.
    2. ETL        -> extract -> transform -> validate -> DQ GATE -> load
                     into database/warehouse.db (etl/pipeline.py, now with
                     the new DQ-gate step wired in).
    3. ML train   -> train/retrain all four models (forecast, segmentation,
                     anomaly, recommendation) and save them to models/
                     (api/train_and_save_models.py — bug-fixed so all four
                     actually get retrained, not just three).
    4. Serve      -> optionally start the unified FastAPI app (DQ +
                     analytics + pipeline-trigger + quality-history
                     endpoints all in one process).

Usage:
    python3 run_all.py                     # steps 1-3, then print how to serve
    python3 run_all.py --serve             # steps 1-3, then start the API on :8000
    python3 run_all.py --skip-dq-check     # skip the standalone DQ report (step 1)
    python3 run_all.py --skip-ml           # skip model (re)training (step 3)
    python3 run_all.py --dq-threshold 70   # loosen/tighten the ETL's DQ gate
"""
import argparse
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("run_all")


class _OnPath:
    """Temporarily puts one subpackage dir at the FRONT of sys.path.
    Needed because dq/main.py and api/main.py share the same module name
    ('main') — without this, whichever subfolder was added to sys.path
    first would shadow the other's `import main`.
    """
    def __init__(self, subdir):
        self.path = os.path.join(HERE, subdir)

    def __enter__(self):
        sys.path.insert(0, self.path)
        # drop any previously-imported 'main' module so the right one loads
        sys.modules.pop("main", None)
        return self

    def __exit__(self, *exc):
        if self.path in sys.path:
            sys.path.remove(self.path)
        sys.modules.pop("main", None)


def step_dq_check():
    logger.info("\n========== [1/5] Data Quality System — sample CSV check ==========")
    os.chdir(os.path.join(HERE, "dq"))
    with _OnPath("dq"):
        from main import _load_dataset, _validate_schema_columns, _validate_and_score
        from config_loader import load_config

        config = load_config("config.json")
        df = _load_dataset("customers_orders.csv")
        _validate_schema_columns(df, config.get("schema", {}), config.get("primary_key"), config.get("date_pairs", []))
        _, score_result = _validate_and_score(
            df, config.get("primary_key"), config.get("schema", {}),
            config.get("date_pairs", []), config.get("scoring_weights"),
        )
    logger.info("Sample dataset DQ score: %.1f / 100", score_result["overall_score"])
    os.chdir(HERE)


def step_etl():
    logger.info("\n========== [2/5] ETL Pipeline (extract -> transform -> validate -> DQ gate -> load) ==========")
    os.chdir(os.path.join(HERE, "etl"))
    with _OnPath("etl"):
        from pipeline import run_pipeline
        run_pipeline(dq_threshold=ARGS.dq_threshold, skip_dq_gate=ARGS.skip_dq_gate)
    os.chdir(HERE)


def step_reviews():
    # NEW: process_reviews.py (unstructured JSON reviews -> fact_reviews with
    # lexicon-based Arabic sentiment) previously had to be run by hand and
    # wasn't part of the "one command runs everything" story. It's optional
    # (the star-schema tables etl.pipeline loads don't depend on it) so a
    # failure here logs a warning instead of aborting the rest of run_all.py.
    logger.info("\n========== [3/5] Reviews & sentiment (unstructured JSON -> fact_reviews) ==========")
    os.chdir(os.path.join(HERE, "etl"))
    with _OnPath("etl"):
        try:
            from process_reviews import process_and_load
            process_and_load()
        except Exception:
            logger.exception("process_reviews.py failed — continuing without fact_reviews.")
    os.chdir(HERE)


def step_ml():
    logger.info("\n========== [4/5] Train & save all ML models ==========")
    os.chdir(os.path.join(HERE, "api"))
    with _OnPath("ml"), _OnPath("api"):
        import importlib
        tsm = importlib.import_module("train_and_save_models")
        tsm.train_and_save_forecasting()
        tsm.train_and_save_segmentation()
        tsm.train_and_save_anomaly()
        tsm.train_and_save_recommendations()
    os.chdir(HERE)


def step_serve():
    logger.info("\n========== [5/5] Serving Unified API on http://0.0.0.0:8000 (docs at /docs) ==========")
    os.chdir(os.path.join(HERE, "api"))
    with _OnPath("api"):
        import uvicorn
        uvicorn.run("main:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Data Platform — run everything end to end")
    parser.add_argument("--skip-dq-check", action="store_true")
    parser.add_argument("--skip-etl", action="store_true")
    parser.add_argument("--skip-reviews", action="store_true", help="skip process_reviews.py (fact_reviews + sentiment)")
    parser.add_argument("--skip-ml", action="store_true")
    parser.add_argument("--skip-dq-gate", action="store_true", help="skip the ETL's DQ gate (load regardless of score)")
    parser.add_argument("--dq-threshold", type=float, default=80.0)
    parser.add_argument("--serve", action="store_true", help="start the unified API after the pipeline finishes")
    ARGS = parser.parse_args()

    if not ARGS.skip_dq_check:
        step_dq_check()
    if not ARGS.skip_etl:
        step_etl()
    if not ARGS.skip_reviews:
        step_reviews()
    if not ARGS.skip_ml:
        step_ml()

    if ARGS.serve:
        step_serve()
    else:
        logger.info(
            "\nDone. To browse the data via the unified API, run:\n"
            "  cd api && uvicorn main:app --reload --port 8000\n"
            "then open http://localhost:8000/docs"
        )
