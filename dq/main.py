"""
Data Quality Validation System — Entry Point (v3)
----------------------------------------------------
Usage:
    python3 main.py --file customers_orders.csv --name customers_orders
    python3 main.py --file customers_orders.csv --name customers_orders --remediate
    python3 main.py --file data.csv --config my_config.json --remediate

What it does:
1. Load settings from config.json (schema, primary key, weights, remediation strategies)
2. Read the data (CSV) with clear error handling
3. Run all quality checks
4. Compute the Data Quality Score
5. (optional) Real auto-remediation of the data + Audit Trail + recompute the score after cleaning
6. Save the result to a historical database (SQLite)
7. Generate an interactive HTML report
"""
import argparse
import logging
import sys

import pandas as pd

from validation import ValidationEngine
from scoring import QualityScorer
from history import HistoryTracker
from reporting_v2 import build_report
from auto_remediation import AutoRemediator
from config_loader import load_config, ConfigError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dq_system.main")


class PipelineError(Exception):
    """An expected pipeline error (missing file, empty data, etc.) — printed as a clear message instead of a raw traceback."""
    pass


def _load_dataset(file_path):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise PipelineError(f"File '{file_path}' was not found. Check the path.")
    except pd.errors.EmptyDataError:
        raise PipelineError(f"File '{file_path}' is completely empty (0 bytes or no columns).")
    except pd.errors.ParserError as e:
        raise PipelineError(f"Could not parse '{file_path}' as valid CSV: {e}")

    if df.empty:
        raise PipelineError(f"File '{file_path}' was read successfully but contains no data rows.")
    return df


def _validate_schema_columns(df, schema, primary_key, date_pairs):
    """Checks that columns mentioned in the config actually exist in the data, and logs a clear warning (not an exception) if they don't."""
    missing_from_data = [col for col in schema if col not in df.columns]
    if missing_from_data:
        logger.warning(f"These columns are in the schema but not in the data, they will be skipped: {missing_from_data}")

    if primary_key and primary_key not in df.columns:
        logger.warning(f"primary_key '{primary_key}' is not in the data — primary key checks will be skipped.")

    for start_col, end_col in date_pairs:
        if start_col not in df.columns or end_col not in df.columns:
            logger.warning(f"Date pair ({start_col}, {end_col}) is incomplete in the data — this consistency check will be skipped.")


def _validate_and_score(df, primary_key, schema, date_pairs, weights=None):
    engine = ValidationEngine(df)
    results = engine.run_all(primary_key=primary_key, schema=schema, date_pairs=date_pairs)
    scorer = QualityScorer(results, total_rows=len(df), weights=weights)
    score_result = scorer.compute()
    return results, score_result


def run_pipeline(file_path, config, dataset_name=None,
                  db_path="dq_history.db", report_path=None,
                  remediate=False, cleaned_csv_path=None, audit_csv_path=None):

    primary_key = config.get("primary_key")
    schema = config.get("schema", {})
    date_pairs = config.get("date_pairs", [])
    weights = config.get("scoring_weights")

    dataset_name = dataset_name or file_path.split("/")[-1]
    report_path = report_path or f"dq_report_{dataset_name.replace('.', '_')}.html"

    # 1) Ingestion
    logger.info(f"Reading file: {file_path}")
    df = _load_dataset(file_path)
    logger.info(f"Read successfully: {len(df)} rows, {len(df.columns)} columns")

    _validate_schema_columns(df, schema, primary_key, date_pairs)

    # 2) Validation + Scoring (before any cleaning)
    logger.info("Running quality checks...")
    results, score_result = _validate_and_score(df, primary_key, schema, date_pairs, weights)
    score_before = score_result["overall_score"]
    logger.info(f"Score before cleaning: {score_before} / 100")

    remediation_info = None
    final_df = df

    # 3) Auto-Remediation (optional)
    if remediate:
        logger.info("Running auto-remediation...")
        strategies = config.get("remediation", {})
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

        final_df = remediator.df
        audit_trail = remediator.get_audit_trail()
        rem_summary = remediator.summary()
        if rem_summary.get("skipped_columns"):
            for s in rem_summary["skipped_columns"]:
                logger.warning(f"Column '{s['column']}' skipped: {s['reason']}")

        results_after, score_result_after = _validate_and_score(final_df, primary_key, schema, date_pairs, weights)

        column_comparison = []
        all_cols = sorted(set(score_result["column_scores"]) | set(score_result_after["column_scores"]))
        for col in all_cols:
            before = score_result["column_scores"].get(col, 100.0)
            after = score_result_after["column_scores"].get(col, 100.0)
            column_comparison.append({
                "column": col, "before": before, "after": after, "delta": round(after - before, 1),
            })

        remediation_info = {
            "score_before": score_before,
            "score_after": score_result_after["overall_score"],
            "total_actions": rem_summary.get("total_actions", 0),
            "rows_removed": rem_summary.get("rows_removed", 0),
            "skipped_columns": rem_summary.get("skipped_columns", []),
            "column_comparison": column_comparison,
            "audit_trail": audit_trail,
        }

        results, score_result = results_after, score_result_after
        logger.info(f"Score after cleaning: {score_result['overall_score']} / 100")

        cleaned_csv_path = cleaned_csv_path or f"cleaned_{dataset_name}"
        audit_csv_path = audit_csv_path or f"audit_trail_{dataset_name}"
        final_df.to_csv(cleaned_csv_path, index=False)
        audit_trail.to_csv(audit_csv_path, index=False)
        logger.info(f"Cleaned data: {cleaned_csv_path} | Audit trail: {audit_csv_path}")

    # 4) Historical Tracking
    tracker = HistoryTracker(db_path=db_path)
    tracker.save_run(
        dataset_name=dataset_name,
        total_rows=len(final_df),
        overall_score=score_result["overall_score"],
        column_scores=score_result["column_scores"],
    )
    history_rows = tracker.get_history(dataset_name=dataset_name)

    # 5) Reporting
    out_path = build_report(
        final_df, results, score_result,
        dataset_name=dataset_name,
        primary_key=primary_key,
        history_rows=history_rows,
        output_path=report_path,
        remediation_info=remediation_info,
    )
    logger.info(f"Report saved to: {out_path}")

    return {
        "results": results,
        "score_result": score_result,
        "report_path": out_path,
        "history_rows": history_rows,
        "remediation_info": remediation_info,
    }


def dry_run_remediation(file_path, config):
    """
    Shows what would happen if remediation actually ran, without modifying or
    saving any file. Useful before running real cleaning on production data.
    """
    primary_key = config.get("primary_key")
    schema = config.get("schema", {})
    date_pairs = config.get("date_pairs", [])
    weights = config.get("scoring_weights")
    strategies = config.get("remediation", {})

    logger.info(f"[DRY RUN] Reading file: {file_path}")
    df = _load_dataset(file_path)
    _validate_schema_columns(df, schema, primary_key, date_pairs)

    results, score_before = _validate_and_score(df, primary_key, schema, date_pairs, weights)

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

    _, score_after = _validate_and_score(remediator.df, primary_key, schema, date_pairs, weights)
    rem_summary = remediator.summary()

    print("\n" + "=" * 60)
    print("DRY RUN — no files were modified or saved")
    print("=" * 60)
    print(f"Score before: {score_before['overall_score']} / 100")
    print(f"Score after (expected): {score_after['overall_score']} / 100")
    print(f"Rows before: {rem_summary['rows_before']} | after: {rem_summary['rows_after']} "
          f"({rem_summary['rows_removed']} row(s) would be removed)")
    print(f"Total expected changes: {rem_summary.get('total_actions', 0)}")
    if rem_summary.get("actions_by_type"):
        print("Breakdown by type:")
        for action, count in rem_summary["actions_by_type"].items():
            print(f"  - {action}: {count}")
    if rem_summary.get("skipped_columns"):
        print("Columns that will be skipped:")
        for s in rem_summary["skipped_columns"]:
            print(f"  - {s['column']}: {s['reason']}")
    print("=" * 60)
    print("Run without --dry-run to actually apply these changes.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality Validation System")
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--name", default=None, help="Dataset name (for tracking)")
    parser.add_argument("--db", default="dq_history.db", help="SQLite history DB path")
    parser.add_argument("--out", default=None, help="Output HTML report path")
    parser.add_argument("--remediate", action="store_true", help="Apply real auto-remediation")
    parser.add_argument("--dry-run", action="store_true",
                         help="Show what remediation would do without actually modifying any file")
    parser.add_argument("--fail-under", type=float, default=None,
                         help="If the final Data Quality Score is below this number, exit with code 2 "
                              "— useful as a data quality gate in a CI/CD pipeline")
    args = parser.parse_args()

    try:
        config = load_config(args.config)

        if args.dry_run:
            if not args.remediate:
                logger.error("--dry-run requires --remediate (there's nothing to dry-run without it).")
                sys.exit(1)
            dry_run_remediation(args.file, config)
            sys.exit(0)

        result = run_pipeline(args.file, config, dataset_name=args.name, db_path=args.db,
                               report_path=args.out, remediate=args.remediate)

        if args.fail_under is not None:
            final_score = result["score_result"]["overall_score"]
            if final_score < args.fail_under:
                logger.error(
                    f"Data Quality Gate failed: final score {final_score} is below the required threshold {args.fail_under}"
                )
                sys.exit(2)
            logger.info(f"Data Quality Gate passed: {final_score} >= {args.fail_under}")

    except (PipelineError, ConfigError) as e:
        logger.error(str(e))
        sys.exit(1)
