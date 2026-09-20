"""
Unit tests for ValidationEngine.
Uses unittest (built-in library) instead of pytest, so it runs with zero
external dependencies.

Run with:
    python3 -m unittest discover -s tests -v
"""
import sys
import os
import unittest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from validation import ValidationEngine


class TestMissingValues(unittest.TestCase):
    def test_no_missing_values(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = ValidationEngine(df).check_missing()
        self.assertEqual(result[0]["missing_count"], 0)
        self.assertEqual(result[0]["severity"], "ok")

    def test_column_entirely_null(self):
        """Edge case: a fully-null column — should be classified critical, not raise."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [np.nan, np.nan, np.nan]})
        result = ValidationEngine(df).check_missing()
        b_report = next(r for r in result if r["column"] == "b")
        self.assertEqual(b_report["missing_pct"], 100.0)
        self.assertEqual(b_report["severity"], "critical")

    def test_empty_dataframe(self):
        """Edge case: a fully empty DataFrame (0 rows) — should not raise ZeroDivisionError."""
        df = pd.DataFrame({"a": []})
        result = ValidationEngine(df).check_missing()
        self.assertEqual(result[0]["missing_pct"], 0)

    def test_warning_vs_critical_threshold(self):
        # 10 rows, one row missing in the column = 10% -> warning (>=5% and <15%)
        df = pd.DataFrame({"a": [1] * 9 + [np.nan]})
        result = ValidationEngine(df).check_missing()
        self.assertEqual(result[0]["severity"], "warning")


class TestDuplicates(unittest.TestCase):
    def test_no_duplicates(self):
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        result = ValidationEngine(df).check_duplicates(primary_key="id")
        self.assertEqual(result["full_row_duplicates"], 0)
        self.assertEqual(result["primary_key_duplicates"], 0)

    def test_all_rows_identical(self):
        """Edge case: every row is a full duplicate."""
        df = pd.DataFrame({"a": [1, 1, 1, 1], "b": ["x", "x", "x", "x"]})
        result = ValidationEngine(df).check_duplicates()
        self.assertEqual(result["full_row_duplicates"], 3)  # every row except the first

    def test_primary_key_duplicates_only(self):
        """The key repeats but the other columns differ — full-row duplicate should be zero."""
        df = pd.DataFrame({"id": [1, 1, 2], "val": ["a", "b", "c"]})
        result = ValidationEngine(df).check_duplicates(primary_key="id")
        self.assertEqual(result["full_row_duplicates"], 0)
        self.assertEqual(result["primary_key_duplicates"], 1)

    def test_primary_key_not_in_columns(self):
        """Edge case: primary_key points to a column that doesn't exist — should not raise, should return zero."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = ValidationEngine(df).check_duplicates(primary_key="nonexistent_col")
        self.assertEqual(result["primary_key_duplicates"], 0)


class TestOutliers(unittest.TestCase):
    def test_no_outliers_in_uniform_data(self):
        df = pd.DataFrame({"a": [10, 11, 10, 12, 11, 10]})
        result = ValidationEngine(df).check_outliers()
        self.assertEqual(result[0]["outlier_count"], 0)

    def test_clear_outlier_detected(self):
        df = pd.DataFrame({"a": [10, 11, 12, 10, 11, 12, 1000]})
        result = ValidationEngine(df).check_outliers()
        self.assertGreaterEqual(result[0]["outlier_count"], 1)

    def test_single_value_column(self):
        """Edge case: a column with a single repeated value — IQR=0, should not raise."""
        df = pd.DataFrame({"a": [5, 5, 5, 5, 5]})
        result = ValidationEngine(df).check_outliers()
        self.assertEqual(result[0]["outlier_count"], 0)

    def test_text_polluted_numeric_column_still_checked(self):
        """
        This was a real bug: a column that's numeric per the schema but whose
        dtype became object due to text contamination (e.g. 'thirty') — it
        should still be checked when a schema is passed.
        """
        df = pd.DataFrame({"age": [25, 30, "thirty", 28, 200, 32]})
        schema = {"age": {"type": "numeric", "min": 0, "max": 120}}
        result = ValidationEngine(df).check_outliers(schema=schema)
        cols_checked = [r["column"] for r in result]
        self.assertIn("age", cols_checked)

    def test_primary_key_excluded(self):
        df = pd.DataFrame({"customer_id": [1, 2, 3, 4, 5], "amount": [10, 12, 11, 13, 12]})
        result = ValidationEngine(df).check_outliers(exclude_columns=["customer_id"])
        cols_checked = [r["column"] for r in result]
        self.assertNotIn("customer_id", cols_checked)
        self.assertIn("amount", cols_checked)


class TestSchema(unittest.TestCase):
    def test_numeric_range_violation(self):
        df = pd.DataFrame({"age": [25, -5, 200, 30]})
        schema = {"age": {"type": "numeric", "min": 0, "max": 120}}
        result = ValidationEngine(df).check_schema(schema)
        self.assertEqual(result[0]["violation_count"], 2)

    def test_email_format_violation(self):
        df = pd.DataFrame({"email": ["a@b.com", "not-an-email", "x@y.co", "bad@"]})
        schema = {"email": {"type": "email"}}
        result = ValidationEngine(df).check_schema(schema)
        self.assertEqual(result[0]["violation_count"], 2)

    def test_schema_column_missing_from_data(self):
        """Edge case: a column mentioned in the schema but not present in the data — should not raise KeyError."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        schema = {"nonexistent": {"type": "numeric"}}
        result = ValidationEngine(df).check_schema(schema)
        self.assertEqual(result, [])


class TestConsistency(unittest.TestCase):
    def test_valid_date_range(self):
        df = pd.DataFrame({
            "start": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "end": pd.to_datetime(["2024-01-05", "2024-02-10"]),
        })
        result = ValidationEngine(df).check_consistency(date_pairs=[("start", "end")])
        self.assertEqual(result[0]["violation_count"], 0)

    def test_end_before_start(self):
        df = pd.DataFrame({
            "start": pd.to_datetime(["2024-01-10", "2024-02-01"]),
            "end": pd.to_datetime(["2024-01-05", "2024-02-10"]),
        })
        result = ValidationEngine(df).check_consistency(date_pairs=[("start", "end")])
        self.assertEqual(result[0]["violation_count"], 1)


class TestRunAll(unittest.TestCase):
    def test_run_all_on_minimal_dataframe(self):
        """Integration test: run_all on a tiny dataset (one row) should not raise."""
        df = pd.DataFrame({"id": [1], "age": [30], "email": ["a@b.com"]})
        engine = ValidationEngine(df)
        results = engine.run_all(
            primary_key="id",
            schema={"age": {"type": "numeric", "min": 0, "max": 120}, "email": {"type": "email"}},
        )
        self.assertIn("missing", results)
        self.assertIn("duplicates", results)
        self.assertIn("outliers", results)
        self.assertIn("schema", results)


if __name__ == "__main__":
    unittest.main()
