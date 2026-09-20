"""
Validation Engine
------------------
Takes a DataFrame + a simple config, and returns a dict with the results
of every check: missing values, duplicates, outliers, schema violations,
consistency checks.
"""
import re
import pandas as pd
import numpy as np


EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


class ValidationEngine:
    def __init__(self, df: pd.DataFrame, config: dict = None):
        self.df = df.copy()
        self.config = config or {}
        self.results = {}

    # ---------- 1) Missing Data ----------
    def check_missing(self, warn_threshold=0.05, critical_threshold=0.15):
        total = len(self.df)
        missing_report = []
        for col in self.df.columns:
            n_missing = self.df[col].isna().sum()
            pct = n_missing / total if total else 0
            if pct >= critical_threshold:
                severity = "critical"
            elif pct >= warn_threshold:
                severity = "warning"
            else:
                severity = "ok"
            missing_report.append({
                "column": col,
                "missing_count": int(n_missing),
                "missing_pct": round(pct * 100, 2),
                "severity": severity,
            })
        self.results["missing"] = missing_report
        return missing_report

    # ---------- 2) Duplicates ----------
    def check_duplicates(self, primary_key: str = None):
        full_dup_mask = self.df.duplicated(keep=False)
        full_dup_count = int(self.df.duplicated(keep="first").sum())

        pk_dup_count = 0
        pk_dup_examples = []
        if primary_key and primary_key in self.df.columns:
            pk_dup_mask = self.df[primary_key].duplicated(keep=False)
            pk_dup_count = int(self.df.loc[pk_dup_mask, primary_key].duplicated(keep="first").sum())
            pk_dup_examples = self.df.loc[pk_dup_mask].sort_values(primary_key).head(10).to_dict("records")

        result = {
            "full_row_duplicates": full_dup_count,
            "full_row_duplicate_examples": self.df.loc[full_dup_mask].head(5).to_dict("records"),
            "primary_key": primary_key,
            "primary_key_duplicates": pk_dup_count,
            "primary_key_duplicate_examples": pk_dup_examples,
        }
        self.results["duplicates"] = result
        return result

    # ---------- 3) Outliers (IQR method) ----------
    def check_outliers(self, numeric_columns=None, schema=None, exclude_columns=None):
        """
        If numeric_columns is given, it's used as-is. Otherwise we combine:
        - columns whose dtype is actually numeric
        - + any column declared as "numeric" in the schema, even if its dtype
          is object because of text contamination (e.g. "thirty" inside an
          age column) — without this, that column would be completely
          excluded from outlier detection even though it needs it the most.
        exclude_columns: columns like the primary key are excluded because
        they are identifiers, not measured values.
        """
        exclude_columns = set(exclude_columns or [])
        if numeric_columns is None:
            numeric_columns = set(self.df.select_dtypes(include=[np.number]).columns.tolist())
            if schema:
                for col, rules in schema.items():
                    if rules.get("type") == "numeric" and col in self.df.columns:
                        numeric_columns.add(col)
            numeric_columns = [c for c in numeric_columns if c not in exclude_columns]

        outlier_report = []
        for col in numeric_columns:
            series = pd.to_numeric(self.df[col], errors="coerce").dropna()
            if series.empty:
                continue
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask = (series < lower) | (series > upper)
            outlier_idx = series[mask].index
            outlier_report.append({
                "column": col,
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2),
                "outlier_count": int(mask.sum()),
                "outlier_pct": round(mask.sum() / len(series) * 100, 2) if len(series) else 0,
                "examples": self.df.loc[outlier_idx, [col]].head(5).to_dict("records"),
            })
        self.results["outliers"] = outlier_report
        return outlier_report

    # ---------- 4) Schema violations ----------
    def check_schema(self, schema: dict):
        """
        schema: {column: {"type": "numeric"/"string"/"date", "min": x, "max": y}}
        """
        violations = []
        for col, rules in schema.items():
            if col not in self.df.columns:
                continue
            expected_type = rules.get("type")
            bad_rows = []

            if expected_type == "numeric":
                coerced = pd.to_numeric(self.df[col], errors="coerce")
                non_numeric_mask = coerced.isna() & self.df[col].notna()
                bad_rows.extend(self.df.loc[non_numeric_mask, [col]].to_dict("records"))

                if "min" in rules or "max" in rules:
                    lo = rules.get("min", -np.inf)
                    hi = rules.get("max", np.inf)
                    out_of_range = coerced.notna() & ((coerced < lo) | (coerced > hi))
                    bad_rows.extend(self.df.loc[out_of_range, [col]].to_dict("records"))

                violations.append({
                    "column": col,
                    "rule": f"numeric, range=[{rules.get('min', '-inf')}, {rules.get('max', 'inf')}]",
                    "violation_count": len(bad_rows),
                    "examples": bad_rows[:5],
                })

            elif expected_type == "email":
                invalid_mask = self.df[col].notna() & ~self.df[col].astype(str).str.match(EMAIL_REGEX)
                bad_rows = self.df.loc[invalid_mask, [col]].to_dict("records")
                violations.append({
                    "column": col,
                    "rule": "valid email format",
                    "violation_count": len(bad_rows),
                    "examples": bad_rows[:5],
                })

        self.results["schema"] = violations
        return violations

    # ---------- 5) Consistency checks ----------
    def check_consistency(self, date_pairs=None):
        """
        date_pairs: [(start_col, end_col), ...] -> checks that start <= end
        """
        date_pairs = date_pairs or []
        consistency_report = []
        for start_col, end_col in date_pairs:
            if start_col not in self.df.columns or end_col not in self.df.columns:
                continue
            start = pd.to_datetime(self.df[start_col], errors="coerce")
            end = pd.to_datetime(self.df[end_col], errors="coerce")
            mask = (start.notna()) & (end.notna()) & (end < start)
            consistency_report.append({
                "check": f"{end_col} >= {start_col}",
                "violation_count": int(mask.sum()),
                "examples": self.df.loc[mask, [start_col, end_col]].head(5).to_dict("records"),
            })
        self.results["consistency"] = consistency_report
        return consistency_report

    # ---------- 6) Format & categorical checks (extra) ----------
    def check_phone_format(self, column, pattern=r"^\+?[0-9\s\-()]{7,15}$"):
        if column not in self.df.columns:
            return None
        regex = re.compile(pattern)
        invalid_mask = self.df[column].notna() & ~self.df[column].astype(str).str.match(regex)
        result = {
            "column": column,
            "rule": "valid phone format",
            "violation_count": int(invalid_mask.sum()),
            "examples": self.df.loc[invalid_mask, [column]].head(5).to_dict("records"),
        }
        self.results.setdefault("schema", []).append(result)
        return result

    def check_text_length(self, column, max_length=255):
        if column not in self.df.columns:
            return None
        too_long_mask = self.df[column].notna() & (self.df[column].astype(str).str.len() > max_length)
        result = {
            "column": column,
            "rule": f"text length <= {max_length}",
            "violation_count": int(too_long_mask.sum()),
            "examples": self.df.loc[too_long_mask, [column]].head(5).to_dict("records"),
        }
        self.results.setdefault("schema", []).append(result)
        return result

    def check_categorical_validity(self, column, allowed_values):
        if column not in self.df.columns:
            return None
        invalid_mask = self.df[column].notna() & ~self.df[column].isin(allowed_values)
        result = {
            "column": column,
            "rule": f"value in {allowed_values}",
            "violation_count": int(invalid_mask.sum()),
            "examples": self.df.loc[invalid_mask, [column]].head(5).to_dict("records"),
        }
        self.results.setdefault("schema", []).append(result)
        return result

    def run_all(self, primary_key=None, schema=None, date_pairs=None):
        self.check_missing()
        self.check_duplicates(primary_key=primary_key)
        self.check_outliers(schema=schema, exclude_columns=[primary_key] if primary_key else None)
        if schema:
            self.check_schema(schema)
        if date_pairs:
            self.check_consistency(date_pairs)
        return self.results
