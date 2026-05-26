# ================================================================
# CLASS 2 — DataTransformer
# ================================================================
# Like a surgeon — implements the treatment plan from the validator.
# Always works on a COPY. Records every change made.
# ================================================================

import sys, pathlib, datetime, logging

from src.Data_Validator import DataValidator
 
_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
 
import pandas as pd
import numpy as np
 
 
# ── Logger setup ────────────────────────────────────────────────
def _setup_logger() -> logging.Logger:
    lgr = logging.getLogger("module05")
    lgr.setLevel(logging.INFO)
    if not lgr.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        lgr.addHandler(h)
    return lgr
 
logger = _setup_logger()
 
 

class DataTransformer:
    """
    Applies documented transformation strategies to fix data quality issues.
 
    Rules:
        - Always works on df.copy() — never the original
        - Records every transformation in self.transformations
        - Uses validated strategies (median for numerics, mode for categoricals)
        - Adds audit trail columns to the output
 
    Attributes
    ──────────
    df               pd.DataFrame    the working copy (modified in place)
    validator        DataValidator   the diagnosis to implement
    transformations  list[dict]      log of every change made
    """
 
    def __init__(self, validator: DataValidator):
        if not validator._checked:
            raise RuntimeError(
                "DataValidator must be run() before DataTransformer"
            )
 
        self.df              = validator.df.copy()   # working copy
        self.validator       = validator
        self.transformations = []                    # audit log
        self._n_rows_start   = len(self.df)
 
        logger.info(
            f"[TRANSFORM] DataTransformer ready — "
            f"{len(self.df):,} rows to transform"
        )
 
    # ── Public entry point ───────────────────────────────────────
    def run(self) -> "DataTransformer":
        """Apply all transformations. Return self for chaining."""
        logger.info("[TRANSFORM] Starting transformations...")
 
        self.drop_duplicates()
        self.fix_nulls()
        self.fix_invalid_values()
        self.fix_consistency()
        self.fix_types()
        self.add_audit_columns()
 
        logger.info(
            f"[TRANSFORM] Complete — "
            f"{len(self.df):,} clean rows "
            f"({self._n_rows_start - len(self.df):,} removed)"
        )
        return self
 
    # ── Transform 1: Drop duplicates ─────────────────────────────
    def drop_duplicates(self) -> "DataTransformer":
        """Remove fully duplicate rows — keep first occurrence."""
        n_before = len(self.df)
        self.df = self.df.drop_duplicates(keep="first").reset_index(drop=True)
        n_removed = n_before - len(self.df)
 
        if n_removed > 0:
            self._record(
                "uniqueness",
                f"Dropped {n_removed:,} duplicate rows (kept first occurrence)"
            )
            logger.info(f"[TRANSFORM] Dropped {n_removed:,} duplicate rows")
        return self
 
    # ── Transform 2: Fix null values ─────────────────────────────
    def fix_nulls(self) -> "DataTransformer":
        """
        Fill nulls using documented strategies:
            - Numeric columns → median (robust to outliers)
            - Text columns    → "Unknown"
            - Boolean columns → False
        """
        for col, null_count in self.validator.null_counts.items():
            if col not in self.df.columns:
                continue
 
            dtype = self.df[col].dtype
 
            if pd.api.types.is_numeric_dtype(dtype):
                fill_value = self.df[col].median()
                self.df[col] = self.df[col].fillna(fill_value)
                self._record(
                    "completeness",
                    f"Filled {null_count:,} null values in '{col}' "
                    f"with median {fill_value:,.2f}"
                )
                logger.info(
                    f"[TRANSFORM] Filled {null_count:,} nulls in "
                    f"'{col}' with median {fill_value:,.2f}"
                )
 
            elif pd.api.types.is_bool_dtype(dtype):
                self.df[col] = self.df[col].fillna(False)
                self._record(
                    "completeness",
                    f"Filled {null_count:,} null booleans in '{col}' with False"
                )
 
            else:
                self.df[col] = self.df[col].fillna("Unknown")
                self._record(
                    "completeness",
                    f"Filled {null_count:,} null strings in '{col}' with 'Unknown'"
                )
                logger.info(
                    f"[TRANSFORM] Filled {null_count:,} nulls in "
                    f"'{col}' with 'Unknown'"
                )
 
        return self
 
    # ── Transform 3: Fix invalid values ──────────────────────────
    def fix_invalid_values(self) -> "DataTransformer":
        """
        Fix out-of-range and placeholder values:
            - Negative fee / number of appoitment  → flag as null (exclude from analysis)
        """

        # negative charge → null
        for number_col in ["unit_cost", "unit_price", "current_stock","reorder_level"]:
            if number_col in self.df.columns:
                mask = pd.to_numeric(self.df[number_col], errors="coerce") < 0
                n    = mask.sum()
                if n > 0:
                    self.df.loc[mask, number_col] = np.nan
                    self._record(
                        "validity",
                        f"Set {n:,} negative values in '{number_col}' to null"
                    )
        # efficiency_pct > 100 → cap at 100
        for pct_col in ["margin_pct", "avg_discount_pct", "return_rate_pct"]:
            if pct_col in self.df.columns:
                mask = pd.to_numeric(
                self.df[pct_col], errors="coerce") > 100
                n    = mask.sum()
                if n > 0:
                    self.df.loc[mask, pct_col] = 100
                    self._record(
                        "validity",
                        f"Capped {n:,} efficiency values exceeding 100% at 100 in {pct_col}"
                    )
 
        return self
 
    # ── Transform 4: Fix consistency ─────────────────────────────
    def fix_consistency(self) -> "DataTransformer":
        """Standardise text columns to title case."""
        text_cols = self.df.select_dtypes(include=["object"]).columns
 
        for col in text_cols:
            # skip high-cardinality free text columns
            if self.df[col].nunique() > 100:
                continue
 
            # find issues recorded for this column
            col_issues = [
                i for i in self.validator.issues
                if i.get("column") == col and i["check"] == "consistency"
            ]
 
            if col_issues:
                self.df[col] = (
                    self.df[col]
                    .astype(str)
                    .str.strip()
                    .str.title()
                )
                self._record(
                    "consistency",
                    f"Standardised '{col}' to title case and stripped whitespace"
                )
                logger.info(
                    f"[TRANSFORM] Standardised '{col}' to title case"
                )
 
        return self
 
    # ── Transform 5: Fix data types ──────────────────────────────
    def fix_types(self) -> "DataTransformer":
        """
        Convert columns to correct types:
            - Date-like columns → datetime
            - Numeric strings  → float/int
        """
        for col in self.df.columns:
            col_lower = col.lower()
 
            # convert date columns
            if any(word in col_lower for word in ["date", "_at", "time"]):
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
                    self._record(
                        "type_correction",
                        f"Converted '{col}' to datetime"
                    )
                    logger.info(
                        f"[TRANSFORM] Converted '{col}' to datetime"
                    )
                except Exception:
                    pass
 
            # convert numeric strings
            elif self.df[col].dtype == object:
                converted = pd.to_numeric(self.df[col], errors="coerce")
                # only apply if most values converted successfully
                if converted.notna().sum() > len(self.df) * 0.8:
                    self.df[col] = converted
                    self._record(
                        "type_correction",
                        f"Converted '{col}' from string to numeric"
                    )
 
        return self
 
    # ── Transform 6: Audit trail columns ─────────────────────────
    def add_audit_columns(self) -> "DataTransformer":
        """
        Add metadata columns so every row is traceable.
        These columns answer: "What was done to this row and when?"
        """
        self.df["_processed_at"]     = datetime.datetime.now().isoformat()
        self.df["_pipeline_version"] = "1.0.0"
        self.df["_n_transforms"]     = len(self.transformations)
 
        self._record(
            "audit",
            "Added audit trail columns: _processed_at, _pipeline_version, _n_transforms"
        )
        logger.info("[TRANSFORM] Audit trail columns added")
        return self
 
    # ── Internal helpers ─────────────────────────────────────────
    def _record(self, check: str, detail: str) -> None:
        """Record a transformation to the audit log."""
        self.transformations.append({
            "check":        check,
            "detail":       detail,
            "timestamp":    datetime.datetime.now().isoformat(),
            "rows_after":   len(self.df),
        })
 
    @property
    def summary(self) -> str:
        """Human-readable transformation summary."""
        lines = [
            "",
            "=" * 60,
            "  TRANSFORMATION SUMMARY",
            "=" * 60,
            f"  Rows before: {self._n_rows_start:,}",
            f"  Rows after:  {len(self.df):,}",
            f"  Removed:     {self._n_rows_start - len(self.df):,}",
            f"  Transforms:  {len(self.transformations)}",
            "",
        ]
        for t in self.transformations:
            lines.append(f"  ✓ {t['check'].upper()}: {t['detail']}")
        lines += ["", "=" * 60, ""]
        return "\n".join(lines)
 
    def __str__(self) -> str:
        return (
            f"DataTransformer("
            f"rows={len(self.df):,}, "
            f"transforms={len(self.transformations)})"
        )
 
    def __repr__(self) -> str:
        return f"DataTransformer(shape={self.df.shape})"
 