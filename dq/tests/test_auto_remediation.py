import sys
import os
import unittest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from validation import ValidationEngine
from auto_remediation import AutoRemediator


class TestAutoRemediator(unittest.TestCase):
    def _run_validation(self, df, primary_key=None, schema=None):
        engine = ValidationEngine(df)
        return engine.run_all(primary_key=primary_key, schema=schema)

    def test_fill_median_on_non_numeric_column_is_skipped_and_logged(self):
        """This was a real bug: fill_median on a text column used to be silently skipped."""
        df = pd.DataFrame({"country": ["Egypt", np.nan, "UAE", np.nan]})
        results = self._run_validation(df)
        rem = AutoRemediator(df, results)
        rem.remediate_missing(column_strategies={"country": "fill_median"})
        self.assertEqual(len(rem.skipped_columns), 1)
        self.assertEqual(rem.skipped_columns[0]["column"], "country")
        # values should remain NaN since the strategy was skipped
        self.assertEqual(rem.df["country"].isna().sum(), 2)

    def test_fill_mode_on_categorical_column(self):
        df = pd.DataFrame({"country": ["Egypt", "Egypt", np.nan, "UAE"]})
        results = self._run_validation(df)
        rem = AutoRemediator(df, results)
        rem.remediate_missing(column_strategies={"country": "fill_mode"})
        self.assertEqual(rem.df["country"].isna().sum(), 0)
        self.assertEqual(rem.df.loc[2, "country"], "Egypt")

    def test_full_and_pk_duplicates_both_removed(self):
        """This was a real bug: full-row and pk dedup used to be alternatives instead of running together."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 3, 3],
            "val": ["a", "a", "b", "c", "d"],   # rows 0&1 are full-row duplicates, rows 3&4 are pk duplicates only
        })
        results = self._run_validation(df, primary_key="id")
        rem = AutoRemediator(df, results)
        rem.remediate_duplicates(strategy="drop_both", primary_key="id")
        # only id=1 (once), id=2, and id=3 (first occurrence) should remain
        self.assertEqual(len(rem.df), 3)
        self.assertEqual(sorted(rem.df["id"].tolist()), [1, 2, 3])

    def test_outlier_capping(self):
        df = pd.DataFrame({"amount": [10, 12, 11, 13, 1000]})
        results = self._run_validation(df)
        rem = AutoRemediator(df, results)
        rem.remediate_outliers(strategy="cap")
        self.assertLess(rem.df["amount"].max(), 1000)

    def test_primary_key_excluded_from_outlier_remediation(self):
        df = pd.DataFrame({"customer_id": [1, 2, 3, 4, 5000], "amount": [10, 11, 12, 11, 10]})
        results = self._run_validation(df, primary_key="customer_id")
        rem = AutoRemediator(df, results)
        rem.remediate_outliers(strategy="cap", exclude_columns=["customer_id"])
        # customer_id should stay unchanged
        self.assertEqual(rem.df["customer_id"].tolist(), [1, 2, 3, 4, 5000])

    def test_audit_trail_is_never_empty_when_actions_happen(self):
        df = pd.DataFrame({"age": [25, np.nan, 30]})
        results = self._run_validation(df)
        rem = AutoRemediator(df, results)
        rem.remediate_missing(column_strategies={"age": "fill_median"})
        trail = rem.get_audit_trail()
        self.assertFalse(trail.empty)
        self.assertIn("reason", trail.columns)


if __name__ == "__main__":
    unittest.main()
