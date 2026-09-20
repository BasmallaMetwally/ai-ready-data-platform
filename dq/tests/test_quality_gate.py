"""
test_quality_gate.py
---------------------
NEW test (added while merging the three projects). Proves the new
integration point — dq/quality_gate.py — actually works both ways:
  1. Clean data passes the gate.
  2. Deliberately bad data (lots of missing values + duplicates) is
     correctly scored low and REJECTED when raise_on_fail=True.

This is the behavior the merged ETL pipeline (etl/pipeline.py) depends
on to block a Load step when incoming data quality is too low.
"""
import os
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from quality_gate import run_quality_gate, QualityGateFailure


class TestQualityGate(unittest.TestCase):
    def setUp(self):
        self.tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name

    def tearDown(self):
        if os.path.exists(self.tmp_db):
            os.remove(self.tmp_db)

    def test_clean_data_passes(self):
        df = pd.DataFrame({
            "customer_id": range(1, 101),
            "age": [30] * 100,
        })
        result = run_quality_gate(
            df, "clean_table", primary_key="customer_id",
            schema={"age": {"type": "numeric", "min": 0, "max": 120}},
            threshold=80.0, history_db=self.tmp_db,
        )
        self.assertTrue(result.passed)
        self.assertGreaterEqual(result.overall_score, 80.0)

    def test_bad_data_is_rejected(self):
        # half the primary keys duplicated + half the ages missing -> should
        # score well under 80 and raise when raise_on_fail=True
        df = pd.DataFrame({
            "customer_id": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
            "age": [None, None, None, None, None, 200, 200, -5, 30, 30],
        })
        with self.assertRaises(QualityGateFailure):
            run_quality_gate(
                df, "dirty_table", primary_key="customer_id",
                schema={"age": {"type": "numeric", "min": 0, "max": 120}},
                threshold=80.0, history_db=self.tmp_db, raise_on_fail=True,
            )

    def test_empty_dataset_scores_zero_and_does_not_crash(self):
        df = pd.DataFrame(columns=["customer_id", "age"])
        result = run_quality_gate(df, "empty_table", threshold=80.0, history_db=self.tmp_db)
        self.assertFalse(result.passed)
        self.assertEqual(result.overall_score, 0.0)

    def test_history_is_recorded(self):
        df = pd.DataFrame({"customer_id": [1, 2, 3], "age": [20, 30, 40]})
        run_quality_gate(df, "history_table", primary_key="customer_id",
                          threshold=0.0, history_db=self.tmp_db)
        from history import HistoryTracker
        rows = HistoryTracker(db_path=self.tmp_db).get_history("history_table")
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
