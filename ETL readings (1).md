# Module 05 — Data Engineering and ETL
## Complete Teaching Guide | The Darko Method 2026

---

## How to Use This Guide

Read this in full. Every section maps to something you will demonstrate live. Students who read this first will understand the *why* behind every line of code — not just the *how*.

---

## Part 1 — What Is Data Engineering?

### The One-Line Definition

Data engineering is the discipline of building systems that move data from where it lives to where it is needed, in the shape it needs to be in, reliably and repeatedly.

### Where Data Engineers Sit in a Modern Data Team

```
Business / Operations
  → generates raw data in databases, files, APIs, sensors

Data Engineer (Module 05)
  → extracts, cleans, structures, loads data

Data Analyst (Module 06)
  → explores, visualises, answers business questions

ML Engineer (Modules 09–10)
  → trains predictive models on clean data

MLOps Engineer (Module 14)
  → deploys and monitors models in production

Business Stakeholders
  → make decisions based on all of the above
```

The data engineer is the foundation. If the data engineer does their job badly, every person downstream works with corrupted information. A machine learning model trained on dirty data is a machine that produces wrong answers with high confidence.

### What Data Engineers Actually Build

- **Pipelines** that extract data from databases, APIs, files
- **Cleaning and validation layers** that detect and fix data quality issues
- **Transformation logic** that reshapes data for analysis and ML
- **Scheduling systems** that run pipelines automatically (daily, hourly)
- **Monitoring** that alerts when pipelines fail or data quality degrades

### The Most Important Mindset Shift

Junior developers write scripts — code that runs once manually. Data engineers build pipelines — code that runs reliably hundreds of times without human intervention.

A script that works once is not a pipeline. A pipeline handles failures gracefully, logs everything it does, can be re-run safely from any point, and produces the same result every time.

---

## Part 2 — ETL: The Core Pattern

### What ETL Stands For

**Extract → Transform → Load**

This three-step pattern has existed since the 1970s. Every data pipeline in the world — from a startup's weekend project to Amazon's petabyte processing — is a variation of it.

### EXTRACT

Get the data from wherever it lives.

Common sources:
- **Relational databases** — PostgreSQL, MySQL, SQL Server — queried with SQL
- **REST APIs** — Stripe, Salesforce, Google Analytics — called with HTTP
- **Files** — CSV, JSON, Parquet, Excel — read from disk or cloud storage
- **Message streams** — Kafka, Kinesis — continuous real-time events
- **Third-party tools** — Fivetran, Airbyte handle extraction automatically

**The extract principle:** Get everything first. Do not filter or clean during extraction. You might discover later that you need data you thought you did not. Always extract to a raw layer, then transform separately.

In our bootcamp, extraction happens in Module 03 (SQL queries → `raw-data.csv`). Module 05 picks up from there.

### TRANSFORM

The heart of data engineering. The question you are always answering: "What needs to be true about this data before it can be trusted?"

Transforms fall into four categories:

**1. Data quality fixes**
- Fill or flag null values with documented strategies
- Remove duplicate rows
- Correct impossible values (negative prices, future birthdates)
- Standardise inconsistent formats (date formats, currency symbols, casing)

**2. Type corrections**
- String `"2024-01-15"` → datetime object
- String `"92000"` → integer 92000
- String `"True"` → boolean True

**3. Business rule enforcement**
- `years_experience = 99` means "unknown" in this system → replace with null
- `salary` must be positive → negative values are data errors, not exceptions
- `efficiency_pct` cannot exceed 100 → cap or flag values above 100

**4. Feature enrichment**
Add derived columns the raw data does not contain:
```python
df["bill_collection_rate"]    = df["amount_collected"] / df["amount_charged"]
df["salary_band"]             = pd.cut(df["salary"], bins=[...])
df["_processed_at"]           = datetime.datetime.now().isoformat()
df["_pipeline_version"]       = "1.0.0"
```

### LOAD

Write the clean data to its destination.

Common destinations:
- **CSV files** — simple, portable, human-readable (our bootcamp standard)
- **Data warehouse** — Snowflake, BigQuery, Redshift — columnar storage at scale
- **Data lake** — S3, Azure Data Lake — raw storage for any format
- **Database** — PostgreSQL, MySQL — for operational queries
- **Feature store** — Feast, Tecton — optimised for ML feature serving

**The load principle:** Always write to a new location. Never overwrite the source. `raw-data.csv` stays unchanged. `processed-data.csv` is written fresh by the pipeline.

### ELT — The Modern Variation

In modern cloud data stacks, the order has flipped to **Extract → Load → Transform**:

1. Extract raw data
2. Load directly into the data warehouse (cheap, fast)
3. Transform inside the warehouse using SQL

Tools like **dbt** (data build tool) run SQL transformations in dependency order inside the warehouse. This is the dominant pattern at companies like Airbnb, GitLab, Shopify.

ETL (transform in Python) vs ELT (transform in SQL) is not a theological debate — it depends on your team's skills and your infrastructure. Know both.

---

## Part 3 — Data Quality: The Full Taxonomy

### Why Data Is Always Dirty

No production database is clean. Data accumulates errors from:
- Human data entry — typos, wrong formats, accidental nulls
- System migrations — old systems used different conventions
- Application bugs — edge cases no developer anticipated
- External sources — third parties have different standards
- Intentional placeholders — `99`, `-1`, `"N/A"` meaning "unknown"
- Schema changes — column renamed, new constraint added

### The Eight Data Quality Dimensions

**1. Completeness — is the value present?**
A null where a value should exist.
Strategy: fill with median (numerics), mode (categoricals), or "Unknown" — document every decision.

**2. Validity — is the value within the allowed range?**
Value exists but cannot be correct given the domain rules.
Examples: `salary = -5000`, `years_experience = 99`, `rating = 7` on a 1–5 scale.

**3. Accuracy — is the value correct?**
Value looks valid but is factually wrong.
`hire_date = 1985` when the company was founded in 2010.
These are the hardest to detect — requires cross-validation against other sources.

**4. Consistency — is the same concept represented the same way?**
`department = "Engineering"` in one table, `"engineering"` in another, `"ENGR"` in a third.
Fix: standardise all to the canonical form using a mapping dictionary.

**5. Uniqueness — are there duplicate records?**
Same row appearing multiple times, from:
- Pipeline re-runs that appended instead of replaced
- Joins producing Cartesian products
- Form submissions processed twice

**6. Timeliness — is the data current?**
Employee marked `is_active = True` who resigned six months ago.
Requires cross-referencing with update timestamps.

**7. Referential integrity — do foreign key relationships hold?**
`sales.employee_id = 999` but there is no employee with ID 999.
The record refers to something that does not exist.

**8. Schema conformance — is the structure what we expect?**
A column was renamed in the source. A new column appeared. A type changed.
Schema drift detection compares incoming schema to expected schema and alerts before transforming.

### The Validate-Before-Transform Principle

Always validate first. Never fix data blindly.

The validation step produces a quality report:
```
DataQualityReport:
  null_salary:              47 rows (3.9%)  → will fill with median £87,000
  negative_salary:           3 rows (0.25%) → will flag and exclude
  invalid_experience:       12 rows (1.0%)  → experience=99, will set to null
  inconsistent_department:  28 rows (2.3%)  → will standardise to title case
  duplicate_rows:            5 rows (0.4%)  → will drop, keep first occurrence
```

This report is evidence. Six months later when someone asks "why does this employee have a null salary?", you have a documented answer.

---

## Part 4 — Building Production-Grade ETL Pipelines

### OOP Design for ETL

Why use classes instead of functions for an ETL pipeline?

**Functions approach:**
```python
raw_df       = extract("raw-data.csv")
validated_df = validate(raw_df)
clean_df     = transform(validated_df)
load(clean_df, "processed-data.csv")
```

This works for a one-off script. But:
- You cannot inspect intermediate state if the pipeline crashes at step 3
- You cannot unit test `validate()` in isolation with controlled inputs
- Adding logging requires threading it through every function
- Re-running from a specific step is difficult

**Class approach:**
```python
validator = DataValidator(df)
validator.check_nulls()
validator.check_ranges()
validator.check_duplicates()
print(validator.report)    # see exactly what was found — inspect state
print(validator.issues)    # a structured list of every problem detected
```

Each class has one responsibility. Each can be tested independently. State is inspectable at any point. The pipeline is a composition of well-tested, well-documented components.

### The Three Core Classes

**DataValidator**
- Reads the raw DataFrame
- Identifies every data quality problem
- Records problems with counts and examples
- Does NOT modify the data
- Produces a quality report
- Like a doctor diagnosing before prescribing

**DataTransformer**
- Reads the DataValidator's report
- Applies documented fix strategies
- Works on a copy of the data (`df.copy()`) — never the original
- Records what was changed and why
- Like the surgeon who implements the treatment plan

**ETLPipeline**
- The orchestrator
- Calls Extractor → Validator → Transformer → Loader in sequence
- Handles pipeline-level errors
- Logs the full run with timing
- Returns a structured result (success/failure, rows processed, issues fixed)

### Method Chaining

```python
result = (
    ETLPipeline()
    .extract()
    .validate()
    .transform()
    .load()
)
```

Each method returns `self`. This reads like a sentence. It is the pandas API pattern — students recognise it because they have been using pandas method chains since Module 02.

### Idempotency — The Most Critical Pipeline Property

**Definition:** Running the pipeline multiple times produces the same result as running it once.

This is not optional. Pipelines fail. They get re-run. If re-running corrupts the output (by appending rows, for example), the pipeline cannot be trusted.

```python
# NOT idempotent — each run appends rows
df.to_csv("processed-data.csv", mode="a", header=False)

# IDEMPOTENT — each run replaces completely
df.to_csv("processed-data.csv", mode="w", index=False)
```

Test for idempotency explicitly:
```python
def test_pipeline_is_idempotent():
    result_1 = run_pipeline()
    result_2 = run_pipeline()
    pd.testing.assert_frame_equal(result_1, result_2)
```

### Error Handling Philosophy

**Fail fast for critical errors:**
If the input file is missing, the database is down, or a required column is absent — stop immediately. Do not produce partial output. Partial output is more dangerous than no output because downstream consumers will not know it is incomplete.

```python
if not PROCESSED_DATA_PATH.parent.exists():
    logger.critical(f"Output directory missing: {PROCESSED_DATA_PATH.parent}")
    raise RuntimeError("Cannot continue — output directory does not exist")
```

**Log and continue for row-level issues:**
If 3 out of 1,200 rows have a null salary, fix the 3 rows and continue. Do not stop the pipeline for a handful of imperfect rows.

```python
n_null_salary = df["salary"].isna().sum()
if n_null_salary > 0:
    median_salary = df["salary"].median()
    df["salary"] = df["salary"].fillna(median_salary)
    logger.warning(f"Filled {n_null_salary} null salaries with median £{median_salary:,.0f}")
```

### Logging — The Pipeline's Black Box

Every significant pipeline action should be logged. When something goes wrong at 3am, logs are the only way to understand what happened.

```python
logger.info("[EXTRACT] Loading raw-data.csv — 1,200 rows, 18 columns")
logger.warning("[VALIDATE] Found 47 null salaries — will fill with median £87,000")
logger.error("[VALIDATE] 3 rows have negative salaries — flagging for exclusion")
logger.info("[TRANSFORM] Transformation complete — 1,150 clean rows")
logger.info("[LOAD] Saved processed-data.csv — 1,150 rows, 21 columns, 1.2 MB")
```

Log level guide:
- `DEBUG`: technical detail (disabled in production)
- `INFO`: normal milestone — pipeline started, file loaded, rows saved
- `WARNING`: something unexpected that was handled — nulls filled, outliers capped
- `ERROR`: something wrong that needs investigation
- `CRITICAL`: pipeline cannot continue

### The Audit Trail

Every row in processed data should be traceable:

```python
df["_processed_at"]      = datetime.datetime.now().isoformat()
df["_pipeline_version"]  = "1.0.0"
df["_source_file"]       = "raw-data.csv"
df["_nulls_filled"]      = total_nulls_fixed
df["_industry"]          = INDUSTRY
```

Six months later: "Why does this employee's salary appear as £87,000 when the original was null?" — you have the answer in the audit trail.

---

## Part 5 — Testing ETL Code

### Why ETL Code Is Undertested (And Shouldn't Be)

ETL bugs are silent. The pipeline runs. It produces output. Nobody notices that 47 salaries were incorrectly filled with the wrong value until six months later when a model is producing wrong predictions.

The solution is the same as all software: automated tests that catch bugs before they reach production.

### What to Test

**Unit tests — per class, per method:**
```python
def test_check_nulls_finds_correct_count():
    df = pd.DataFrame({"salary": [50000, None, 80000, None, 120000]})
    validator = DataValidator(df)
    validator.check_nulls()
    assert validator.null_counts["salary"] == 2

def test_fill_nulls_uses_median_not_mean():
    # With outlier: mean=200,000, median=80,000
    df = pd.DataFrame({"salary": [50000, None, 80000, 110000, 1000000]})
    transformer = DataTransformer(df)
    transformer.fill_nulls()
    assert transformer.df["salary"].isna().sum() == 0
    assert abs(transformer.df.iloc[1]["salary"] - 80000) < 1  # median, not mean
```

**Integration tests — full pipeline:**
```python
def test_pipeline_produces_output_with_correct_shape():
    pipeline = ETLPipeline()
    result   = pipeline.extract().validate().transform().load()
    df       = pd.read_csv(PROCESSED_DATA_PATH)
    assert len(df) > 0
    assert "salary" in df.columns
    assert df["salary"].isna().sum() == 0  # no nulls after processing
```

**Data quality tests — output properties:**
```python
def test_no_negative_salaries_in_output():
    df = pd.read_csv(PROCESSED_DATA_PATH)
    assert (df["salary"] >= 0).all()

def test_row_count_within_expected_range():
    df = pd.read_csv(PROCESSED_DATA_PATH)
    assert 800 <= len(df) <= 1300  # reasonable range for the dataset
```

### Test Data Strategy

Never test on production data. Create synthetic DataFrames inside tests with exactly the problems you want to test:

```python
def make_dirty_df():
    return pd.DataFrame({
        "employee_id":       [1, 2, 2, 3, 4],    # row 2 duplicated
        "salary":            [90000, None, -500, 150000, 999999],  # null, negative, outlier
        "department":        ["Engineering", "sales", None, "HR", "engineering"],
        "years_experience":  [5, 99, 3, 15, 8],  # 99 is a system default
    })
```

---

## Part 6 — Scheduling and Orchestration (Industry Context)

### How Pipelines Run in Production

ETL pipelines do not run manually in production. They run on a schedule:
- Every hour — real-time operational data
- Every morning at 6am — daily analytics refresh
- Every Sunday at midnight — weekly reports

**Cron (Linux scheduling):**
```
0 6 * * *  python /opt/pipelines/etl/run.py
```
Runs at 6am every day. Simple, reliable. Still used everywhere.

### Workflow Orchestration Tools

For complex pipelines with tasks that depend on each other:

**Apache Airflow** — the industry standard for 10+ years. Pipelines defined as Python DAGs (Directed Acyclic Graphs). Each task depends on others. Airflow handles scheduling, retries, logging, alerting, backfilling. Used at Airbnb, Twitter, LinkedIn, NASA, NASA.

**Prefect** — modern Python-first alternative. Less infrastructure overhead than Airflow.

**dbt** — specifically for SQL transforms inside a data warehouse. Defines dependencies between SQL models. Has built-in testing, documentation, lineage. The dominant tool in the modern data stack.

**Cloud managed services:**
- AWS Glue — managed Spark ETL
- Azure Data Factory — visual pipeline builder
- Google Dataflow — Apache Beam managed service

### The Modern Data Stack Architecture

```
Sources (PostgreSQL, Stripe API, Salesforce)
    │
    ▼ Extraction (Fivetran, Airbyte — managed connectors)
    │
    ▼ Raw Layer (S3, Azure Data Lake — store everything as-is)
    │
    ▼ Transform (dbt inside Snowflake/BigQuery — SQL transformations)
    │
    ▼ Serving Layer (APIs, dashboards, ML feature stores)
```

Our Module 05 pipeline is a single-machine, Python version of this architecture. The concepts are identical. The scale is different.

---

## Part 7 — Industry Best Practices Reference

| Practice | Why It Matters |
|---|---|
| Validate before transforming | Know what you are fixing before you fix it |
| Never modify the source data | The original must always be recoverable |
| Log every significant action | You need a trail when something goes wrong at 3am |
| Build idempotent pipelines | Re-running must always be safe |
| Add audit trail columns | Know what was done to each row and when |
| Test with synthetic data | Never test on real customer data |
| Fail fast on critical errors | Partial output is worse than no output |
| Document every transformation decision | The team needs to understand the logic 6 months from now |
| Version your pipelines | Know which version processed which data |
| Keep raw and processed data separate | Never mix uncleaned and cleaned data |
| Use schema validation | Detect when the source structure changes before it causes silent errors |
| Implement alerts on failure | Someone must be woken up at 3am if the pipeline fails |

---

## Part 8 — Common Interview Questions

**"What is the difference between ETL and ELT?"**
ETL transforms before loading — transformation logic lives in Python or Spark. ELT loads raw data to the warehouse first, then transforms using SQL. ELT is preferred when the data warehouse is powerful and cheap (BigQuery, Snowflake). ETL is preferred when transformation logic is complex and better expressed in Python.

**"What is idempotency and why does it matter for data pipelines?"**
An idempotent pipeline produces the same result no matter how many times it runs. It matters because pipelines fail and must be re-run. If re-running doubles the data or produces different results, the pipeline cannot be trusted in production.

**"How do you handle a pipeline failure mid-run?"**
Checkpointing — save progress at each step so a re-run starts from the last successful checkpoint rather than the beginning. For atomic operations (all-or-nothing), use database transactions.

**"What is data lineage?"**
The ability to trace a piece of data from its original source through every transformation to its final form. Essential for debugging ("why is this salary wrong?"), compliance (GDPR right to explanation), and impact analysis (if I change this transformation, what reports are affected?).

**"How do you detect schema drift?"**
Compare the incoming DataFrame's columns, types, and value distributions against an expected schema stored at pipeline build time. Alert if columns are missing, renamed, or have unexpected types.

**"What would you do if a column in the source database was renamed?"**
Schema drift detection should alert before the pipeline runs on invalid data. The fix: update the pipeline to handle the new column name, add the old name as an alias for backwards compatibility, version the pipeline change.

---

## What Comes Next

After Module 05, students have `processed-data.csv` — clean, typed, enriched, audited. This file is the input to every subsequent module:

- **Module 06** — EDA explores and analyses it
- **Module 09** — ML trains models on it
- **Module 11** — LLM is given context from it
- **Module 12** — RAG indexes it for question answering
- **Module 14** — MLOps uses its statistics as the drift baseline

The ETL pipeline is the foundation of the entire AI data product.

