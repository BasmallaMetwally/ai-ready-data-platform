"""
Historical Tracking
--------------------
Every time a validation run happens, we save the overall score + per-column
score to a local SQLite database, so we can track how data quality evolves
over time (improving / degrading).
"""
import sqlite3
from datetime import datetime, timezone


class HistoryTracker:
    def __init__(self, db_path="dq_history.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    run_timestamp TEXT NOT NULL,
                    total_rows INTEGER,
                    overall_score REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS column_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    column_name TEXT NOT NULL,
                    score REAL,
                    FOREIGN KEY(run_id) REFERENCES quality_runs(run_id)
                )
            """)

    def save_run(self, dataset_name, total_rows, overall_score, column_scores: dict):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "INSERT INTO quality_runs (dataset_name, run_timestamp, total_rows, overall_score) "
                "VALUES (?, ?, ?, ?)",
                (dataset_name, datetime.now(timezone.utc).isoformat(), total_rows, overall_score),
            )
            run_id = cur.lastrowid
            for col, score in column_scores.items():
                conn.execute(
                    "INSERT INTO column_scores (run_id, column_name, score) VALUES (?, ?, ?)",
                    (run_id, col, score),
                )
            conn.commit()
        return run_id

    def get_history(self, dataset_name=None, limit=50):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if dataset_name:
                rows = conn.execute(
                    "SELECT * FROM quality_runs WHERE dataset_name = ? "
                    "ORDER BY run_timestamp DESC LIMIT ?",
                    (dataset_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM quality_runs ORDER BY run_timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
