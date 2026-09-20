"""
Great Expectations Adapter (optional)
----------------------------------------
This isn't a replacement for our ValidationEngine — it's an optional
adapter that converts config.json (our schema) into a Great Expectations
Expectation Suite, for anyone who needs to integrate this project into a
larger stack that already uses GE (or wants to use GE tooling like the
ready-made Data Docs for reporting).

Note: this file is written correctly against the documented GE API
(v0.18+), but I haven't run it as thoroughly as the rest of the project
(e.g. `api.py` and `validation.py`, which have real unit tests). If you're
going to use it, install great-expectations and add a few tests of your
own before relying on it in production.

Usage:
    pip install great-expectations
    python3 ge_adapter.py --file customers_orders.csv --config config.json
"""
import argparse
import sys

try:
    import great_expectations as gx
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False

import pandas as pd
from config_loader import load_config


def build_expectation_suite_from_config(config: dict, suite_name="dq_system_suite"):
    """
    Converts config.json (schema + primary_key) into a list of "expectations"
    independent of the GE library (list of dicts), so you can inspect/edit it
    even without GE installed, and then, if GE is available, actually build
    it as an ExpectationSuite.
    """
    expectations = []

    primary_key = config.get("primary_key")
    if primary_key:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": primary_key},
        })
        expectations.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": primary_key},
        })

    for col, rules in config.get("schema", {}).items():
        if rules.get("type") == "numeric":
            kwargs = {"column": col}
            if "min" in rules:
                kwargs["min_value"] = rules["min"]
            if "max" in rules:
                kwargs["max_value"] = rules["max"]
            expectations.append({
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": kwargs,
            })
        elif rules.get("type") == "email":
            expectations.append({
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": col, "regex": r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"},
            })

    return {"suite_name": suite_name, "expectations": expectations}


def run_with_great_expectations(df: pd.DataFrame, config: dict):
    if not GE_AVAILABLE:
        raise ImportError(
            "great_expectations is not installed. Run: pip install great-expectations\n"
            "The expectation suite (list of dicts) can still be generated without it via "
            "build_expectation_suite_from_config() if you just want to inspect or export it."
        )

    suite_dict = build_expectation_suite_from_config(config)

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("dq_system_runtime")
    data_asset = data_source.add_dataframe_asset(name="dataset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name=suite_dict["suite_name"])
    for exp in suite_dict["expectations"]:
        expectation_cls = getattr(gx.expectations, _to_class_name(exp["expectation_type"]))
        suite.add_expectation(expectation_cls(**exp["kwargs"]))

    result = batch.validate(suite)
    return result


def _to_class_name(expectation_type: str) -> str:
    # expect_column_values_to_be_unique -> ExpectColumnValuesToBeUnique
    return "".join(part.capitalize() for part in expectation_type.split("_"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Great Expectations adapter (optional)")
    parser.add_argument("--file", required=True)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--print-suite-only", action="store_true",
                         help="Print the expectation suite (JSON) without trying to actually run GE")
    args = parser.parse_args()

    config = load_config(args.config)
    suite = build_expectation_suite_from_config(config)

    if args.print_suite_only or not GE_AVAILABLE:
        import json
        if not GE_AVAILABLE:
            print("(great_expectations is not installed here — printing the generated suite only, without running it)\n")
        print(json.dumps(suite, ensure_ascii=False, indent=2))
        sys.exit(0)

    df = pd.read_csv(args.file)
    result = run_with_great_expectations(df, config)
    print(result)
