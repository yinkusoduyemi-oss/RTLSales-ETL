# RTL Sales Analytics — ETL Pipeline
### Mojola Oduyemi 

---

## Overview

A production-grade Extract → Transform → Load pipeline built for the retail industry. This project extracts raw sales, inventory, product, returns and store data from a PostgreSQL database, validates and cleans it, and produces a processed dataset ready for exploratory data analysis and machine learning.

---

## ETL Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│                                                                      │
│   retail.sales      retail.products    retail.stores                 │
│   retail.inventory  retail.returns                                   │
│                                             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  SQL extraction query
                           │  (extractor.sql — CTEs + JOINs)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    EXTRACT                             │
│                                                                      │
│   SQLQueryRunner          DataExtractor                             │
│   ─────────────           ─────────────                             │
│   • Connects to DB        • Calls run_file("extractor.sql")         │
│   • Executes SQL          • Handles DB unavailable → synthetic data  │
│   • Returns DataFrame     • Saves raw-data.csv                      │
│   • Logs timing           • Reports data quality issues             │
│                                                                      │
│   OUTPUT → data/raw/raw-data.csv  (12,731 rows × 46 columns)       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    VALIDATE  (Module 05 — Step 1)                    │
│                                                                      │
│   DataValidator                                                      │
│   ─────────────                                                      │
│   check_schema()       Records column names and types               │
│   check_nulls()        Counts nulls per column                      │
│   check_duplicates()   Detects fully duplicate rows                 │
│   check_ranges()       Flags invalid values:                        │
│                          • negative unit_cost / gross_revenue       │
│                          • margin_pct or discount > 100%            │
│                          • return_rate_pct > 100%                   │
│                          • stock_status = 'NO DATA'                 │
│                          • duplicate columns (.1 suffix)            │
│   check_consistency()  Detects mixed casing in text columns         │
│                                                                      │
│   OUTPUT → DataQualityReport (printed + stored in validator.issues) │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSFORM  (Module 05 — Step 2)                   │
│                                                                      │
│   DataTransformer                                                    │
│   ────────────────                                                   │
│   drop_duplicates()    Remove exact duplicate rows (keep first)     │
│   fix_nulls()          • Numeric columns  → fill with median        │
│                        • Text columns     → fill with "Unknown"     │
│                        • Boolean columns  → fill with False         │
│   fix_invalid_values() • Negative values  → set to null            │
│                        • efficiency > 100 → cap at 100             │
│                        • Placeholder 99   → set to null            │
│   fix_consistency()    Standardise text to title case + strip       │
│   fix_types()          Convert date columns to datetime             │
│                        Convert numeric strings to float             │
│   add_audit_columns()  Add _processed_at, _pipeline_version,       │
│                            _n_transforms                            │
│                                                                      │
│   PRINCIPLE: Always works on df.copy() — never modifies source      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    LOAD  (Module 05 — Step 3)                        │
│                                                                      │
│   ETLPipeline.load()                                                │
│   ──────────────────                                                │
│   • Writes processed-data.csv (mode="w" — idempotent)              │
│   • Never modifies raw-data.csv                                     │
│   • Creates output directory if missing                             │
│   • Prints final pipeline summary report                            │
│                                                                      │
│   OUTPUT → data/processed/processed-data.csv                        │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 DOWNSTREAM  (Module 06+)                             │
│                                                                      │
│   EDAEngine          → group analysis, correlation, time trends     │
│   AnomalyDetector    → IQR + Z-score consensus anomaly detection    │
│   ML Pipeline        → feature engineering, model training          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
ETL-RTLSales-Analytics-Adeyinka/
│
├── config.py                   # shared settings — paths, DB, logger
├── run.py                      # entry point: python run.py
├── requirements.txt            # package dependencies
├── .env                        # secrets — never committed to Git
├── .gitignore
│
├── src/
│   ├── __init__.py
│   ├── Data_Validator.py       # diagnoses data quality issues
│   ├── Data_Transformer.py     # fixes data quality issues
│   └── etl_pipeline.py         # orchestrates the full pipeline
│
├── data/
│   ├── raw/
│   │   └── raw-data.csv        # extracted from PostgreSQL
│   └── processed/
│       └── processed-data.csv  # clean output — input to Module 06
│
└── test.ipynb                  # interactive testing notebook
```

---

## Data Sources

| Table | Rows (approx) | Key fields |
|-------|--------------|-----------|
| `retail.sales` | 30,000+ | sale_id, store_id, product_id, total_amount |
| `retail.products` | 1,000+ | product_id, category, brand, unit_cost |
| `retail.stores` | 13 | store_id, store_name, region, city |
| `retail.inventory` | 5,000+ | store_id, product_id, quantity, reorder_level |
| `retail.returns` | 5,000+ | return_id, sale_id, reason, refund_amount |

---

## Pipeline Classes

### `DataValidator`
Diagnoses data quality issues without modifying anything. Like a doctor — inspects and reports only.

```python
validator = DataValidator(df)
validator.run()
print(validator.report)
```

### `DataTransformer`
Implements fixes based on the validator's findings. Always works on a copy. Records every change.

```python
transformer = DataTransformer(validator)
transformer.run()
print(transformer.summary)
```

### `ETLPipeline`
Orchestrates the full pipeline with method chaining.

```python
pipeline = ETLPipeline()
pipeline.extract().validate().transform().load()
```

---

## Data Quality Checks

| Check | Dimension | Action |
|-------|-----------|--------|
| Null values | Completeness | Fill with median / "Unknown" |
| Duplicate rows | Uniqueness | Drop — keep first |
| Negative revenue/cost | Validity | Set to null |
| Discount > 100% | Validity | Flag as error |
| Mixed casing | Consistency | Standardise to title case |
| Duplicate columns (.1) | Schema | Flag for SQL query fix |
| stock_status = 'NO DATA' | Completeness | Flag missing inventory |

---


## Author

**Mojola Oduyemi** 