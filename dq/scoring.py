"""
Scoring System
--------------
Takes the ValidationEngine's results and converts every problem into a
deduction from 100 points, then returns an overall score + a per-column
score.
"""


class QualityScorer:
    # Weight of each problem type (tunable to business priorities, or via config.json)
    WEIGHTS = {
        "missing_critical": 15,
        "missing_warning": 5,
        "full_row_duplicate": 10,
        "pk_duplicate": 15,
        "outlier": 8,
        "schema_violation": 12,
        "consistency_violation": 15,
    }

    def __init__(self, results: dict, total_rows: int, weights: dict = None):
        self.results = results
        self.total_rows = max(total_rows, 1)
        self.deductions = []  # log of every deduction applied and why
        # weights can come from config.json instead of staying hardcoded
        self.WEIGHTS = {**self.WEIGHTS, **(weights or {})}

    def _deduct(self, reason, points, column=None):
        self.deductions.append({"reason": reason, "points": points, "column": column})

    def compute(self):
        score = 100.0
        column_penalties = {}

        def add_col_penalty(col, points):
            column_penalties[col] = column_penalties.get(col, 0) + points

        # Missing values
        for item in self.results.get("missing", []):
            if item["severity"] == "critical":
                pts = self.WEIGHTS["missing_critical"] * (item["missing_pct"] / 100)
                score -= pts
                add_col_penalty(item["column"], pts)
                self._deduct(f"High missing rate ({item['missing_pct']}%)", round(pts, 2), item["column"])
            elif item["severity"] == "warning":
                pts = self.WEIGHTS["missing_warning"] * (item["missing_pct"] / 100)
                score -= pts
                add_col_penalty(item["column"], pts)
                self._deduct(f"Notable missing rate ({item['missing_pct']}%)", round(pts, 2), item["column"])

        # Duplicates
        dup = self.results.get("duplicates", {})
        if dup:
            full_dup_ratio = dup.get("full_row_duplicates", 0) / self.total_rows
            pts = self.WEIGHTS["full_row_duplicate"] * min(full_dup_ratio * 10, 1)
            if pts > 0:
                score -= pts
                self._deduct(f"{dup.get('full_row_duplicates', 0)} fully duplicated row(s)", round(pts, 2))

            pk_dup_ratio = dup.get("primary_key_duplicates", 0) / self.total_rows
            pts_pk = self.WEIGHTS["pk_duplicate"] * min(pk_dup_ratio * 10, 1)
            if pts_pk > 0:
                score -= pts_pk
                self._deduct(f"{dup.get('primary_key_duplicates', 0)} duplicate(s) in the primary key", round(pts_pk, 2))

        # Outliers
        for item in self.results.get("outliers", []):
            ratio = item["outlier_pct"] / 100
            pts = self.WEIGHTS["outlier"] * min(ratio * 5, 1)
            if pts > 0:
                score -= pts
                add_col_penalty(item["column"], pts)
                self._deduct(f"{item['outlier_count']} outlier value(s)", round(pts, 2), item["column"])

        # Schema violations
        for item in self.results.get("schema", []):
            ratio = item["violation_count"] / self.total_rows
            pts = self.WEIGHTS["schema_violation"] * min(ratio * 10, 1)
            if pts > 0:
                score -= pts
                add_col_penalty(item["column"], pts)
                self._deduct(f"{item['violation_count']} rule violation(s) ({item['rule']})", round(pts, 2), item["column"])

        # Consistency
        for item in self.results.get("consistency", []):
            ratio = item["violation_count"] / self.total_rows
            pts = self.WEIGHTS["consistency_violation"] * min(ratio * 10, 1)
            if pts > 0:
                score -= pts
                self._deduct(f"{item['violation_count']} consistency violation(s) ({item['check']})", round(pts, 2))

        overall_score = max(round(score, 1), 0)

        # Per-column score (100 - total deductions on it)
        column_scores = {
            col: max(round(100 - pts, 1), 0) for col, pts in column_penalties.items()
        }

        return {
            "overall_score": overall_score,
            "column_scores": column_scores,
            "deductions": sorted(self.deductions, key=lambda d: -d["points"]),
        }
