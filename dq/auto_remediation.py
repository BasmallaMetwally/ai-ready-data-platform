"""
Auto-Remediation Module (v2 — P1 bug fixes)
------------------------------------------------
Applies real cleaning (not just suggestions) to the data, and logs every
decision (or every skip) to an Audit Trail, so the transparency is actually
complete, not just a slogan.

Changes vs v1:
- remediate_missing now takes a strategy *per column* (dict) instead of one
  strategy for every column, because "fill_median" doesn't make sense for a
  text column like email or country.
- Any column that gets skipped (no strategy defined for it, or the strategy
  isn't suitable for its type) is logged to the audit trail with an explicit
  reason, instead of being silently skipped.
- remediate_duplicates now runs full-row dedup and primary-key dedup
  together by default (drop_both) instead of them being alternatives to
  each other.
"""
import pandas as pd
import numpy as np


class AutoRemediator:
    def __init__(self, df: pd.DataFrame, results: dict):
        self.original_df = df.copy()
        self.df = df.copy()
        self.results = results
        self.audit_log = []  # every change or skip: index, column, old_value, new_value, action, reason
        self.skipped_columns = []  # separate log for columns that were skipped entirely

    def _log(self, row_index, column, old_value, new_value, action, reason):
        self.audit_log.append({
            "row_index": row_index,
            "column": column,
            "old_value": old_value,
            "new_value": new_value,
            "action": action,
            "reason": reason,
        })

    def _log_skip(self, column, reason):
        self.skipped_columns.append({"column": column, "reason": reason})
        self._log(None, column, "<no strategy applied>", "<unchanged>", "skipped", reason)

    # ---------- Missing values ----------
    def remediate_missing(self, column_strategies: dict = None, default_strategy: str = "flag_only"):
        """
        column_strategies: {"age": "fill_median", "country": "fill_mode", "email": "flag_only"}
        Any column with missing values that isn't listed in column_strategies gets
        default_strategy. default_strategy="flag_only" (not fill_median) because we
        can't assume every column is numeric.
        """
        column_strategies = column_strategies or {}
        missing_cols = [m["column"] for m in self.results.get("missing", []) if m["missing_count"] > 0]

        for col in missing_cols:
            if col not in self.df.columns:
                continue
            strategy = column_strategies.get(col, default_strategy)
            null_idx = self.df[self.df[col].isna()].index

            if strategy == "drop_row":
                for idx in null_idx:
                    self._log(idx, col, None, "<row dropped>", "drop_row", "missing value")
                self.df = self.df.drop(index=null_idx)

            elif strategy == "fill_median":
                numeric = pd.to_numeric(self.df[col], errors="coerce")
                # If the column has few or no valid numeric values, fill_median doesn't apply
                # — log an explicit skip instead of silently doing nothing.
                numeric_ratio = numeric.notna().sum() / max(len(self.df[col].dropna()), 1)
                if numeric.notna().sum() == 0:
                    self._log_skip(col, "fill_median is not applicable: no valid numeric value in this column")
                    continue
                if numeric_ratio < 0.5:
                    self._log_skip(
                        col,
                        f"fill_median is not suitable: this column looks mostly text-based "
                        f"(only {round(numeric_ratio*100,1)}% of values are numeric) — use fill_mode or flag_only instead"
                    )
                    continue
                fill_val = numeric.median()
                self.df[col] = numeric  # any invalid text (e.g. "thirty") becomes NaN here too
                combined_null_idx = self.df[self.df[col].isna()].index
                for idx in combined_null_idx:
                    self._log(idx, col, None, fill_val, "fill_median", "missing or invalid value")
                self.df.loc[combined_null_idx, col] = fill_val

            elif strategy == "fill_mode":
                mode_vals = self.df[col].mode(dropna=True)
                if mode_vals.empty:
                    self._log_skip(col, "fill_mode is not applicable: the column is entirely empty")
                    continue
                fill_val = mode_vals.iloc[0]
                for idx in null_idx:
                    if idx in self.df.index:
                        self._log(idx, col, None, fill_val, "fill_mode", "missing value")
                self.df.loc[null_idx, col] = fill_val

            elif strategy == "flag_only":
                flag_col = f"{col}__was_missing"
                self.df[flag_col] = self.df.index.isin(null_idx)
                for idx in null_idx:
                    self._log(idx, col, None, None, "flag_only", "flagged only, value left unchanged")

            else:
                self._log_skip(col, f"unknown strategy: '{strategy}'")

        return self.df

    # ---------- Duplicates ----------
    def remediate_duplicates(self, strategy="drop_both", primary_key=None):
        """
        strategy: "drop_both" (default — full-row then primary-key) | "drop_full" | "drop_by_key" | "flag_only"
        drop_both runs both together, not as alternatives, because a full-row
        duplicate problem and a primary-key duplicate problem are two different
        problems that can coexist.
        """
        apply_full = strategy in ("drop_both", "drop_full")
        apply_by_key = strategy in ("drop_both", "drop_by_key") and primary_key and primary_key in self.df.columns

        if apply_full:
            dup_mask = self.df.duplicated(keep="first")
            for idx in self.df[dup_mask].index:
                self._log(idx, None, "<full row>", "<row dropped>", "drop_full_duplicate", "fully duplicated row")
            self.df = self.df[~dup_mask]

        if apply_by_key:
            dup_mask = self.df[primary_key].duplicated(keep="first")
            for idx in self.df[dup_mask].index:
                self._log(idx, primary_key, self.df.loc[idx, primary_key], "<row dropped>",
                           "drop_by_key", f"duplicate in primary key {primary_key}")
            self.df = self.df[~dup_mask]

        if strategy == "flag_only":
            self.df["__is_duplicate"] = self.df.duplicated(keep=False)
            if primary_key and primary_key in self.df.columns:
                self.df["__pk_is_duplicate"] = self.df[primary_key].duplicated(keep=False)

        if not apply_full and not apply_by_key and strategy != "flag_only":
            self._log_skip("<duplicates>", f"unknown duplicates strategy or missing primary_key: '{strategy}'")

        return self.df

    # ---------- Outliers ----------
    def remediate_outliers(self, strategy="cap", columns=None, exclude_columns=None):
        """
        exclude_columns: columns like the primary key are excluded from
        outlier checking/remediation because they are identifiers, not
        measured values (it makes no sense for customer_id to be a
        "statistical outlier").
        """
        exclude_columns = set(exclude_columns or [])
        outlier_cols = columns or [
            o["column"] for o in self.results.get("outliers", [])
            if o["outlier_count"] > 0 and o["column"] not in exclude_columns
        ]

        for col in outlier_cols:
            if col not in self.df.columns or col in exclude_columns:
                continue
            series = pd.to_numeric(self.df[col], errors="coerce")
            valid = series.dropna()
            if valid.empty:
                continue
            q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            mask = series.notna() & ((series < lower) | (series > upper))
            affected_idx = self.df[mask].index

            if strategy == "cap":
                for idx in affected_idx:
                    old_val = self.df.loc[idx, col]
                    new_val = lower if series[idx] < lower else upper
                    self._log(idx, col, old_val, new_val, "cap_outlier", f"value outside [{round(lower,2)}, {round(upper,2)}]")
                    self.df.loc[idx, col] = new_val

            elif strategy == "drop":
                for idx in affected_idx:
                    self._log(idx, col, self.df.loc[idx, col], "<row dropped>", "drop_outlier", "outlier value")
                self.df = self.df.drop(index=affected_idx)

            elif strategy == "flag_only":
                flag_col = f"{col}__is_outlier"
                self.df[flag_col] = self.df.index.isin(affected_idx)
                for idx in affected_idx:
                    self._log(idx, col, self.df.loc[idx, col], None, "flag_only", "outlier value flagged only")

            else:
                self._log_skip(col, f"unknown outliers strategy: '{strategy}'")

        return self.df

    def get_audit_trail(self) -> pd.DataFrame:
        return pd.DataFrame(self.audit_log)

    def summary(self):
        trail = self.get_audit_trail()
        base = {
            "rows_before": len(self.original_df),
            "rows_after": len(self.df),
            "rows_removed": len(self.original_df) - len(self.df),
            "skipped_columns": self.skipped_columns,
        }
        if trail.empty:
            base["total_actions"] = 0
            return base
        applied = trail[trail["action"] != "skipped"]
        base["total_actions"] = len(applied)
        base["actions_by_type"] = applied["action"].value_counts().to_dict()
        base["skipped_count"] = len(trail[trail["action"] == "skipped"])
        return base
