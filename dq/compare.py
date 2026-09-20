"""
Comparison Mode
-----------------
Compares data quality between two versions of the same dataset (e.g. last
month's version and today's), in a single report, showing what improved
and what got worse.

Usage:
    python3 compare.py --old old_data.csv --new new_data.csv --config config.json
"""
import argparse
import logging
import sys

from config_loader import load_config, ConfigError
from main import _load_dataset, _validate_schema_columns, _validate_and_score, PipelineError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("dq_system.compare")


def compare_datasets(old_path, new_path, config):
    primary_key = config.get("primary_key")
    schema = config.get("schema", {})
    date_pairs = config.get("date_pairs", [])
    weights = config.get("scoring_weights")

    logger.info(f"Reading old version: {old_path}")
    old_df = _load_dataset(old_path)
    _validate_schema_columns(old_df, schema, primary_key, date_pairs)
    old_results, old_score = _validate_and_score(old_df, primary_key, schema, date_pairs, weights)

    logger.info(f"Reading new version: {new_path}")
    new_df = _load_dataset(new_path)
    _validate_schema_columns(new_df, schema, primary_key, date_pairs)
    new_results, new_score = _validate_and_score(new_df, primary_key, schema, date_pairs, weights)

    # Compare at the overall score level
    overall_delta = round(new_score["overall_score"] - old_score["overall_score"], 1)

    # Compare at the per-column level
    all_cols = sorted(set(old_score["column_scores"]) | set(new_score["column_scores"]))
    column_comparison = []
    for col in all_cols:
        before = old_score["column_scores"].get(col, 100.0)
        after = new_score["column_scores"].get(col, 100.0)
        column_comparison.append({"column": col, "old": before, "new": after, "delta": round(after - before, 1)})

    # Compare the count of problems of each type
    def _count(results, key, subkey=None):
        items = results.get(key, [])
        if key == "duplicates":
            return items.get("full_row_duplicates", 0) + items.get("primary_key_duplicates", 0)
        return sum(item.get("outlier_count", item.get("violation_count", item.get("missing_count", 0))) for item in items)

    problem_comparison = {
        "missing": {"old": _count(old_results, "missing"), "new": _count(new_results, "missing")},
        "duplicates": {"old": _count(old_results, "duplicates"), "new": _count(new_results, "duplicates")},
        "outliers": {"old": _count(old_results, "outliers"), "new": _count(new_results, "outliers")},
        "schema": {"old": _count(old_results, "schema"), "new": _count(new_results, "schema")},
        "consistency": {"old": _count(old_results, "consistency"), "new": _count(new_results, "consistency")},
    }

    return {
        "old_rows": len(old_df), "new_rows": len(new_df),
        "old_score": old_score["overall_score"], "new_score": new_score["overall_score"],
        "overall_delta": overall_delta,
        "column_comparison": column_comparison,
        "problem_comparison": problem_comparison,
    }


def print_comparison_report(result):
    trend = "improved 📈" if result["overall_delta"] > 0 else ("got worse 📉" if result["overall_delta"] < 0 else "unchanged ➖")
    print("\n" + "=" * 60)
    print("Data Quality Comparison")
    print("=" * 60)
    print(f"Rows: {result['old_rows']} → {result['new_rows']}")
    print(f"Overall Score: {result['old_score']} → {result['new_score']}  ({result['overall_delta']:+}) {trend}")
    print("\nBy problem type (count):")
    for kind, vals in result["problem_comparison"].items():
        delta = vals["new"] - vals["old"]
        arrow = "↓" if delta < 0 else ("↑" if delta > 0 else "=")
        print(f"  {kind:12s}: {vals['old']:4d} → {vals['new']:4d}  ({arrow} {abs(delta)})")
    print("\nBy column (score):")
    for c in sorted(result["column_comparison"], key=lambda x: x["delta"]):
        if c["delta"] != 0:
            print(f"  {c['column']:15s}: {c['old']:5.1f} → {c['new']:5.1f}  ({c['delta']:+.1f})")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare data quality between two dataset versions")
    parser.add_argument("--old", required=True, help="Path to the older CSV version")
    parser.add_argument("--new", required=True, help="Path to the newer CSV version")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    args = parser.parse_args()

    try:
        config = load_config(args.config)
        result = compare_datasets(args.old, args.new, config)
        print_comparison_report(result)
    except (PipelineError, ConfigError) as e:
        logger.error(str(e))
        sys.exit(1)
