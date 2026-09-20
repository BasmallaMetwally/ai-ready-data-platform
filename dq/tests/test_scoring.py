import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scoring import QualityScorer


class TestQualityScorer(unittest.TestCase):
    def test_perfect_data_scores_100(self):
        results = {"missing": [], "duplicates": {}, "outliers": [], "schema": [], "consistency": []}
        score = QualityScorer(results, total_rows=100).compute()
        self.assertEqual(score["overall_score"], 100)
        self.assertEqual(score["deductions"], [])

    def test_critical_missing_deducts_points(self):
        results = {
            "missing": [{"column": "email", "missing_count": 50, "missing_pct": 50.0, "severity": "critical"}],
            "duplicates": {}, "outliers": [], "schema": [], "consistency": [],
        }
        score = QualityScorer(results, total_rows=100).compute()
        self.assertLess(score["overall_score"], 100)
        self.assertIn("email", score["column_scores"])

    def test_score_never_goes_below_zero(self):
        """Edge case: way too many problems — the score should floor at zero, never go negative."""
        results = {
            "missing": [{"column": c, "missing_count": 100, "missing_pct": 100.0, "severity": "critical"} for c in "abcdefgh"],
            "duplicates": {"full_row_duplicates": 100, "primary_key_duplicates": 100},
            "outliers": [{"column": "x", "outlier_count": 100, "outlier_pct": 100.0}],
            "schema": [{"column": "y", "rule": "numeric", "violation_count": 100}],
            "consistency": [{"check": "a>=b", "violation_count": 100}],
        }
        score = QualityScorer(results, total_rows=100).compute()
        self.assertGreaterEqual(score["overall_score"], 0)

    def test_zero_total_rows_no_crash(self):
        """Edge case: total_rows=0 (empty dataset) — should not cause a division by zero."""
        results = {"missing": [], "duplicates": {"full_row_duplicates": 0}, "outliers": [], "schema": [], "consistency": []}
        score = QualityScorer(results, total_rows=0).compute()
        self.assertEqual(score["overall_score"], 100)

    def test_deductions_sorted_by_severity(self):
        results = {
            "missing": [
                {"column": "a", "missing_count": 5, "missing_pct": 5.0, "severity": "warning"},
                {"column": "b", "missing_count": 90, "missing_pct": 90.0, "severity": "critical"},
            ],
            "duplicates": {}, "outliers": [], "schema": [], "consistency": [],
        }
        score = QualityScorer(results, total_rows=100).compute()
        # the biggest deduction should be first in the list
        self.assertGreaterEqual(score["deductions"][0]["points"], score["deductions"][-1]["points"])


if __name__ == "__main__":
    unittest.main()
