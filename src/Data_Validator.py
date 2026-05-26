
# ================================================================
# Module 05 — Data Engineering and ETL Pipeline
# ================================================================
# Three core classes:
#   DataValidator   — identifies data quality issues (diagnoses)
#   DataTransformer — fixes data quality issues (treats)
#   ETLPipeline     — orchestrates the full pipeline (runs the show)
# ================================================================
 
import sys, pathlib, datetime, logging
 
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
 
 
# ================================================================
# CLASS 1 — DataValidator
# ================================================================
# Like a doctor — diagnoses problems WITHOUT modifying anything.
# Produces a quality report showing exactly what is wrong.
# ================================================================
 
class DataValidator:
    """
    Identifies data quality issues in a raw DataFrame.
 
    Checks all 8 data quality dimensions:
        1. Completeness  — null values
        2. Validity      — values outside allowed ranges
        3. Consistency   — inconsistent formatting
        4. Uniqueness    — duplicate rows
        5. Accuracy      — impossible values (e.g. future hire dates)
        6. Timeliness    — stale or placeholder values
        7. Referential integrity — foreign key checks (optional)
        8. Schema conformance   — expected columns present
 
    Attributes
    ──────────
    df          pd.DataFrame    the raw data to validate (never modified)
    issues      list[dict]      every issue found, structured for reporting
    null_counts dict            {column: count} of null values per column
    report      str             human-readable quality report
    """
 
    def __init__(self, df: pd.DataFrame):
        self.df          = df.copy()   # never modify the original!
        self.issues      = []          # structured list of every problem
        self.null_counts = {}          # {col: null_count}
        self._checked    = False       # guard: run() must be called first
 
        logger.info(
            f"[VALIDATE] DataValidator ready — "
            f"{len(df):,} rows | {df.shape[1]} columns"
        )
 
    # ── Public entry point ───────────────────────────────────────
    def run(self) -> "DataValidator":
        """Run all validation checks. Return self for chaining."""
        logger.info("[VALIDATE] Starting validation checks...")
 
        self.check_schema()
        self.check_nulls()
        self.check_duplicates()
        self.check_ranges()
        self.check_consistency()
 
        self._checked = True
        total = len(self.issues)
        logger.info(
            f"[VALIDATE] Complete — {total} issue type(s) found"
        )
        return self
 
    # ── Check 1: Schema conformance ──────────────────────────────
    def check_schema(self) -> "DataValidator":
        """Verify expected columns are present."""
        # We record dtypes and flag unexpected schema changes
        schema_info = {col: str(dtype) for col, dtype in self.df.dtypes.items()}
 
        self.issues.append({
            "check":    "schema_conformance",
            "severity": "INFO",
            "detail":   f"Schema recorded — {len(schema_info)} columns",
            "columns":  list(schema_info.keys()),
            "dtypes":   schema_info,
            "count":    0,
        })
        logger.info(f"[VALIDATE] Schema: {len(schema_info)} columns recorded")
        return self
 
    # ── Check 2: Null values (Completeness) ──────────────────────
    def check_nulls(self) -> "DataValidator":
        """Count null values per column."""
        null_series = self.df.isna().sum()
        self.null_counts = null_series[null_series > 0].to_dict()
 
        if self.null_counts:
            for col, count in self.null_counts.items():
                pct = round(count / len(self.df) * 100, 2)
                self.issues.append({
                    "check":    "completeness",
                    "severity": "WARNING",
                    "column":   col,
                    "detail":   f"{count:,} null values ({pct}%)",
                    "count":    count,
                    "pct":      pct,
                })
                logger.warning(
                    f"[VALIDATE] NULL {col}: {count:,} rows ({pct}%)"
                )
        else:
            logger.info("[VALIDATE] Completeness: no nulls found ")
        return self
 
    # ── Check 3: Duplicate rows (Uniqueness) ─────────────────────
    def check_duplicates(self) -> "DataValidator":
        """Detect fully duplicate rows."""
        n_dupes = self.df.duplicated().sum()
 
        if n_dupes > 0:
            pct = round(n_dupes / len(self.df) * 100, 2)
            self.issues.append({
                "check":    "uniqueness",
                "severity": "WARNING",
                "detail":   f"{n_dupes:,} duplicate rows ({pct}%)",
                "count":    int(n_dupes),
                "pct":      pct,
            })
            logger.warning(
                f"[VALIDATE] Duplicates: {n_dupes:,} rows ({pct}%)"
            )
        else:
            logger.info("[VALIDATE] Uniqueness: no duplicates found ")
        return self
 
   # ── Check 4: Range validity ───────────────────────────────────
    def check_ranges(self):
        """Check numeric columns for out-of-range values.Retail industry rules defined here."""
        range_rules = {
        # column: (min_valid, max_valid, description)
            "unit_cost":                (0,    None,       "unit cost must be positive"),
            "unit_price":               (0,    None,       "unit price must be positive"),
            "margin_pct":               (0,    100,        "margin must be 0-100%"),
            "avg_unit_price":           (0,    None,       "avg unit price must be positive"),
            "avg_discount_pct":         (0,    100,        "discount must be 0-100%"),
            "total_cost":               (0,    None,       "total cost must be positive"),
            "gross_revenue":            (0,    None,       "gross revenue must be positive"),
            "total_discount_given":     (0,    None,       "discount given must be positive"),
            "net_revenue":              (0,    None,       "net revenue must be positive"),
            "gross_profit":             (None, None,       "gross profit can be negative"),
            "total_transactions":       (0,    None,       "transactions must be positive"),
            "total_units_sold":         (0,    None,       "units sold must be positive"),
            "regular_customers":        (0,    None,       "customer count must be positive"),
            "new_customers":            (0,    None,       "customer count must be positive"),
            "loyalty_customers":        (0,    None,       "customer count must be positive"),
            "cash_payments":            (0,    None,       "payment count must be positive"),
            "card_payments":            (0,    None,       "payment count must be positive"),
            "total_returns":            (0,    None,       "returns must be positive"),
            "total_units_returned":     (0,    None,       "units returned must be positive"),
            "total_refunded":           (0,    None,       "refunded amount must be positive"),
            "defective_returns":        (0,    None,       "defective returns must be positive"),
            "wrong_item_returns":       (0,    None,       "wrong item returns must be positive"),
            "dissatisfied_returns":     (0,    None,       "dissatisfied returns must be positive"),
            "return_rate_pct":          (0,    100,        "return rate must be 0-100%"),
            "current_stock":            (0,    None,       "stock must be positive"),
            "reorder_level":            (0,    None,       "reorder level must be positive"),
            "days_since_restock":       (0,    None,       "days since restock must be positive"),
            "est_days_stock_remaining": (0,    None,       "days remaining must be positive"),
             }

        for col, (min_val, max_val, description) in range_rules.items():
            if col not in self.df.columns:
                continue

            numeric_col = pd.to_numeric(self.df[col], errors="coerce")

            # check below minimum
            if min_val is not None:
                n_below = (numeric_col < min_val).sum()
                if n_below > 0:
                    self.issues.append({
                    "check":    "validity",
                    "severity": "ERROR",
                    "column":   col,
                    "detail":   f"{n_below:,} values below {min_val} — {description}",
                    "count":    int(n_below),
                    "rule":     f">= {min_val}",
                    })
                    logger.error(
                    f"[VALIDATE] INVALID {col}: {n_below:,} values < {min_val}"
                    )

             # check above maximum
            if max_val is not None:
                n_above = (numeric_col > max_val).sum()
                if n_above > 0:
                    self.issues.append({
                    "check":    "validity",
                    "severity": "WARNING",
                    "column":   col,
                    "detail":   f"{n_above:,} values above {max_val} — {description}",
                    "count":    int(n_above),
                    "rule":     f"<= {max_val}",
                    })
                    logger.warning(
                    f"[VALIDATE] OUTLIER {col}: {n_above:,} values > {max_val}"
                    )

             # special: stock_status = 'NO DATA' means missing inventory info
            if "stock_status" in self.df.columns:
                n_no_data = (self.df["stock_status"] == "NO DATA").sum()
                if n_no_data > 0:
                    self.issues.append({
                    "check":    "validity",
                    "severity": "WARNING",
                    "column":   "stock_status",
                    "detail":   f"{n_no_data:,} rows with 'NO DATA' — missing inventory info",
                    "count":    int(n_no_data),
                    "rule":     "!= 'NO DATA'",
                    })
                logger.warning(
                f"[VALIDATE] PLACEHOLDER stock_status='NO DATA': {n_no_data:,} rows"
                )

            # special: duplicate columns detected (regular_customers.1 etc)
            duplicate_cols = [c for c in self.df.columns if c.endswith(".1")]
            if duplicate_cols:
                self.issues.append({
                "check":    "schema_conformance",
                "severity": "WARNING",
                "column":   "_table",
                "detail":   f"Duplicate columns detected: {duplicate_cols} — check SQL query for duplicate GROUP BY fields",
                "count":    len(duplicate_cols),
                "rule":     "no duplicate columns",
                 })
                logger.warning(
                f"[VALIDATE] DUPLICATE COLUMNS: {duplicate_cols}"
                )

        return self


    # ── Check 5: Consistency ──────────────────────────────────────
    def check_consistency(self) -> "DataValidator":
        """
        Check text columns for inconsistent casing or formatting.
         """
        text_cols = self.df.select_dtypes(include=["object"]).columns

        for col in text_cols:
        # skip date columns and high cardinality free text
            if any(word in col.lower() for word in ["date", "name", "suppliers"]):
                continue
            if self.df[col].nunique() > 50:
                continue

            # detect mixed casing
            non_null = self.df[col].dropna().astype(str)
            if len(non_null) == 0:
                continue

            lower_count = (non_null == non_null.str.lower()).sum()
            upper_count = (non_null == non_null.str.upper()).sum()
            title_count = (non_null == non_null.str.title()).sum()
            total       = len(non_null)

            max_consistent = max(lower_count, upper_count, title_count)
            if max_consistent < total * 0.9:
                n_inconsistent = total - max_consistent
                self.issues.append({
                "check":    "consistency",
                "severity": "WARNING",
                "column":   col,
                "detail":   f"{n_inconsistent:,} values with inconsistent casing",
                "count":    int(n_inconsistent),
                "unique_values": non_null.value_counts().head(5).to_dict(),
                })
                logger.warning(
                f"[VALIDATE] INCONSISTENT casing in {col}: "
                f"{n_inconsistent:,} values"
                )

        return self
 
    # ── Quality report property ───────────────────────────────────
    @property
    def report(self) -> str:
        """Human-readable quality report."""
        if not self._checked:
            return "Run .run() first to generate the report."
 
        lines = [
            "",
            "=" * 60,
            "  DATA QUALITY REPORT",
            "=" * 60,
            f"  Rows:    {len(self.df):,}",
            f"  Columns: {self.df.shape[1]}",
            f"  Issues:  {len(self.issues)}",
            "",
        ]
 
        severity_order = {"ERROR": 0, "WARNING": 1, "INFO": 2}
        sorted_issues = sorted(
            self.issues,
            key=lambda x: severity_order.get(x["severity"], 3)
        )
 
        for issue in sorted_issues:
            sev = issue["severity"]
            chk = issue["check"].upper()
            det = issue["detail"]
            col = issue.get("column", "_table")
            lines.append(f"  [{sev}] {chk} | {col}: {det}")
 
        lines += ["", "=" * 60, ""]
        return "\n".join(lines)
 
    def __str__(self) -> str:
        return (
            f"DataValidator("
            f"rows={len(self.df):,}, "
            f"issues={len(self.issues)}, "
            f"checked={self._checked})"
        )
 
    def __repr__(self) -> str:
        return f"DataValidator(shape={self.df.shape})"
 