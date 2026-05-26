
# ================================================================
# CLASS 3 — ETLPipeline
# ================================================================
# The orchestrator — runs Extract → Validate → Transform → Load
# in sequence. Handles pipeline-level errors. Logs everything.
# ================================================================

import sys, pathlib, datetime, logging


 
_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
 
import pandas as pd
import numpy as np
 
from config import engine, RAW_DATA_PATH, logger,PROC_DATA_PATH

 
from src.Data_Validator import DataValidator

from src.Data_Transformer import DataTransformer

 
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
 


class ETLPipeline:
    """
    Orchestrates the full ETL pipeline.
 
    Steps:
        1. extract()   — load raw-data.csv
        2. validate()  — run DataValidator, print quality report
        3. transform() — run DataTransformer, fix issues
        4. load()      — save processed-data.csv with audit trail
 
    Attributes
    ──────────
    raw_path        pathlib.Path    path to raw-data.csv
    processed_path  pathlib.Path    path to write processed-data.csv
    raw_df          pd.DataFrame    loaded raw data
    validator       DataValidator   quality check results
    transformer     DataTransformer transformation results
    clean_df        pd.DataFrame    final clean data
    """
 
    def __init__(
        self,
        raw_path:       pathlib.Path = None,
        processed_path: pathlib.Path = None,
        ):
        # default paths — look for data/ folder relative to this file
        #_here = pathlib.Path(__file__).resolve().parent
        self.raw_path       = raw_path       or (RAW_DATA_PATH/ "raw-data.csv")
        self.processed_path = processed_path or (PROC_DATA_PATH/ "processed-data.csv")
 
        self.raw_df      = None
        self.validator   = None
        self.transformer = None
        self.clean_df    = None
        self._status     = "ready"
        self._start_time = None
 
        logger.info(
            f"[ETL] Pipeline ready | "
            f"raw: {self.raw_path.name} → "
            f"processed: {self.processed_path.name}"
        )
 
    # ── Step 1: Extract ──────────────────────────────────────────
    def extract(self) -> "ETLPipeline":
        """
        Load raw-data.csv into self.raw_df.
        Fails fast if the file is missing — partial output is worse than no output.
        """
        self._start_time = datetime.datetime.now()
        logger.info(f"[EXTRACT] Loading: {self.raw_path}")
 
        # FAIL FAST — critical error if file missing
        if not self.raw_path.exists():
            logger.critical(
                f"[EXTRACT] File not found: {self.raw_path}"
            )
            raise FileNotFoundError(
                f"Raw data file not found: {self.raw_path}\n"
                f"Run Module 03 first to generate raw-data.csv"
            )
 
        self.raw_df  = pd.read_csv(self.raw_path, encoding="utf-8")
        self._status = "extracted"
 
        logger.info(
            f"[EXTRACT] Loaded {len(self.raw_df):,} rows × "
            f"{self.raw_df.shape[1]} columns"
        )
        return self
 
    # ── Step 2: Validate ─────────────────────────────────────────
    def validate(self) -> "ETLPipeline":
        """Run DataValidator and print the quality report."""
        if self.raw_df is None:
            raise RuntimeError("Call extract() before validate()")
 
        self.validator = DataValidator(self.raw_df)
        self.validator.run()
 
        # always print the quality report — this is the evidence trail
        print(self.validator.report)
 
        self._status = "validated"
        return self
 
    # ── Step 3: Transform ────────────────────────────────────────
    def transform(self) -> "ETLPipeline":
        """Run DataTransformer using the validator's findings."""
        if self.validator is None:
            raise RuntimeError("Call validate() before transform()")
 
        self.transformer = DataTransformer(self.validator)
        self.transformer.run()
        self.clean_df = self.transformer.df
 
        # print transformation summary
        print(self.transformer.summary)
 
        self._status = "transformed"
        return self
 
    # ── Step 4: Load ─────────────────────────────────────────────
    def load(self) -> "ETLPipeline":
        """
        Save processed-data.csv.
 
        Idempotent: always writes fresh (mode="w"), never appends.
        The raw file is NEVER modified.
        """
        if self.clean_df is None:
            raise RuntimeError("Call transform() before load()")
 
        # ensure output directory exists
        self.processed_path.parent.mkdir(parents=True, exist_ok=True)
 
        # IDEMPOTENT write — same result every time
        self.clean_df.to_csv(
            self.processed_path,
            index=False,
            encoding="utf-8",
            mode="w",        # always overwrite — never append
        )
 
        file_size_kb = self.processed_path.stat().st_size / 1024
        duration     = (datetime.datetime.now() - self._start_time ).total_seconds()
 
        self._status = "loaded"
        logger.info(
            f"[LOAD] Saved {len(self.clean_df):,} rows to "
            f"{self.processed_path.name} "
            f"({file_size_kb:.1f} KB)"
        )
 
        # final pipeline report
        self._print_final_report(duration, file_size_kb)
        return self
 
    # ── Final report ─────────────────────────────────────────────
    def _print_final_report(
        self, duration: float, file_size_kb: float
    ) -> None:
        """Print the final pipeline summary."""
        n_removed = (
            len(self.raw_df) - len(self.clean_df)
            if self.raw_df is not None else 0
        )
 
        print()
        print("=" * 60)
        print("  MODULE 05 — ETL PIPELINE COMPLETE")
        print("=" * 60)
        print(f"  Duration:        {duration:.1f}s")
        print(f"  Raw rows:        {len(self.raw_df):,}")
        print(f"  Clean rows:      {len(self.clean_df):,}")
        print(f"  Rows removed:    {n_removed:,}")
        print(f"  Columns:         {self.clean_df.shape[1]}")
        print(f"  Output:          {self.processed_path.name}")
        print(f"  File size:       {file_size_kb:.1f} KB")
        print(f"  Issues found:    {len(self.validator.issues)}")
        print(f"  Transforms run:  {len(self.transformer.transformations)}")
        print()
        print("  NEXT STEP: Module 06 EDA uses processed-data.csv")
        print("=" * 60)
 
    def __str__(self) -> str:
        return (
            f"ETLPipeline("
            f"status={self._status!r}, "
            f"raw={self.raw_path.name!r})"
        )
 
    def __repr__(self) -> str:
        return f"ETLPipeline(status={self._status!r})"
 
 
