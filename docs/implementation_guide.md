# Implementation Guide — Food Waste Optimization 360

A complete technical reference covering every component of the pipeline: data generation, Bronze/Silver/Gold layers, star schema warehouse, Athena analytics, Airflow orchestration, Streamlit dashboard, and testing.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Layout](#2-repository-layout)
3. [Environment Setup](#3-environment-setup)
4. [Data Generation](#4-data-generation)
5. [Bronze Layer — Ingestion](#5-bronze-layer--ingestion)
6. [Silver Layer — Transform](#6-silver-layer--transform)
7. [Quality Checks — Shared DQ Gate](#7-quality-checks--shared-dq-gate)
8. [Gold Layer — Dimension Loaders](#8-gold-layer--dimension-loaders)
9. [Gold Layer — SCD2 Supplier Dimension](#9-gold-layer--scd2-supplier-dimension)
10. [Gold Layer — Fact Loaders](#10-gold-layer--fact-loaders)
11. [Athena Analytics](#11-athena-analytics)
12. [Root Cause Classification View](#12-root-cause-classification-view)
13. [Airflow Orchestration](#13-airflow-orchestration)
14. [AWS Infrastructure Setup](#14-aws-infrastructure-setup)
15. [CI/CD — GitHub Actions](#15-cicd--github-actions)
16. [Test Suite](#16-test-suite)
17. [Dashboard](#17-dashboard)
18. [Makefile Commands](#18-makefile-commands)
19. [Cost Profile](#19-cost-profile)
20. [Known Bugs Fixed](#20-known-bugs-fixed)

---

## 1. Project Overview

**Food Waste Optimization 360** is a batch data engineering platform that ingests synthetic kitchen operational data, processes it through a medallion architecture (Bronze → Silver → Gold), models a star schema warehouse, and surfaces waste insights via Athena SQL and a Streamlit dashboard.

### High-Level Data Flow

```
[5 CSV Sources — Synthetic]
  macro_production_logs.csv   (10,000 rows)
  macro_waste_logs.csv         (6,000 rows)
  macro_menu_data.csv            (200 rows)
  macro_location_data.csv         (20 rows)
  macro_supplier_data.csv         (~68 rows)
         │
         ▼  ingestion/bronze_loader.py  (boto3 + pandas — Glue Python shell)
[S3 Bronze Layer]
  s3://bucket/bronze/source={name}/date={YYYY-MM-DD}/data.parquet
         │
         ▼  transforms/silver_transform.py  (PySpark — Glue Spark job)
[S3 Silver Layer]
  s3://bucket/silver/year={Y}/month={M}/
         │
         ├──▶ warehouse/dim_loaders.py     (pandas — Glue Python shell)
         ├──▶ warehouse/scd2_supplier.py   (pandas — reads Bronze directly)
         └──▶ warehouse/fact_loaders.py    (pandas)
                    │
                    ▼
[S3 Gold Layer — Star Schema]
  s3://bucket/gold/dims/{dim_name}/data.parquet
  s3://bucket/gold/facts/{fact_name}/data.parquet
         │
         ▼  AWS Athena (food_waste_db — external Parquet tables)
[Analytics Layer]
  5 analytical queries + waste_root_cause view
         │
         ▼  Streamlit (PyAthena)
[Dashboard — 5 pages]
```

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Compute | AWS Glue (Python shell + Spark) |
| Storage | AWS S3 (Parquet) |
| Warehouse | AWS Athena (external tables) |
| Orchestration | Apache Airflow via Astro CLI (local Docker) |
| Dashboard | Streamlit + PyAthena |
| Testing | pytest + PySpark (in-memory) |
| CI/CD | GitHub Actions |

---

## 2. Repository Layout

```
food-waste-360/
├── ingestion/
│   ├── data_generator.py        # Faker-based CSV generation
│   └── bronze_loader.py         # S3 upload + Parquet conversion
├── transforms/
│   ├── silver_transform.py      # PySpark Silver job
│   └── quality_checks.py        # Shared DQ gate (Silver + Gold)
├── warehouse/
│   ├── dim_loaders.py           # 6 SCD1 + static dims
│   ├── scd2_supplier.py         # DIM_SUPPLIER SCD2 logic
│   └── fact_loaders.py          # 4 fact tables
├── analytics/
│   ├── athena_queries.sql        # 5 core analytical queries
│   └── root_cause_view.sql       # Root cause classification view
├── orchestration/
│   └── dags/
│       └── food_waste_pipeline.py
├── aws_setup/
│   ├── 01_s3_setup.py
│   └── 02_iam_setup.py
├── tests/
│   ├── test_quality_checks.py
│   ├── test_silver_transforms.py
│   └── test_scd2.py
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── implementation_guide.md   ← this file
├── data/raw/                     # local CSV output from data_generator.py
├── .github/workflows/pipeline_tests.yml
├── .env.example
├── .gitignore
├── requirements.txt
├── Makefile
└── setup_aws.py
```

---

## 3. Environment Setup

### Required environment variables

Copy `.env.example` to `.env` and fill in values. **Never commit `.env` to git.**

```bash
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-south-1
S3_BUCKET=food-waste-360-596234624522
ATHENA_RESULTS_BUCKET=food-waste-360-596234624522-athena-results
ATHENA_DATABASE=food_waste_db
```

All Python files load credentials exclusively via `os.environ.get()` — no hardcoded values anywhere.

### AWS resources (already provisioned)

| Resource | Value |
|----------|-------|
| S3 bucket | `food-waste-360-596234624522` |
| Athena results bucket | `food-waste-360-596234624522-athena-results` |
| IAM role | `FoodWasteGlueRole` |
| Glue job — Bronze | `food_waste_bronze` |
| Glue job — Silver | `food_waste_silver` |
| Glue job — Gold | `food_waste_gold` |
| Athena workgroup | `food-waste-wg` (1 GB scan limit) |
| Athena database | `food_waste_db` |
| Region | `ap-south-1` (Mumbai) |

---

## 4. Data Generation

**File:** [ingestion/data_generator.py](../ingestion/data_generator.py)

Generates five referentially consistent synthetic CSV files using **Faker** (Indian locale `en_IN`) with a fixed seed (`random.seed(42)`, `Faker.seed(42)`) for reproducibility.

### Generation order and referential integrity

```
1. generate_location_data()   →  20 locations (LOC001–LOC020)
2. generate_menu_data()       →  200 menu items (MI0001–MI0200)
3. generate_supplier_data()   →  ~68 rows (10 suppliers × 3-8 items + ~30% SCD2 history rows)
4. generate_production_logs() →  10,000 rows sampled from locations × menu × dates × meal_periods
5. generate_waste_logs()      →  6,000 rows sampled from production rows (waste ≤ prepared enforced)
```

**Key constraint enforced at generation time:**

```python
# Waste logs are always sampled from actual production rows
sampled = prod_df.sample(n=min(n_rows, len(prod_df)), replace=False)
for _, prod_row in sampled.iterrows():
    max_waste = prod_row["quantity_prepared"]
    waste_qty = round(random.uniform(1.0, max_waste * 0.45), 1)  # max 45% waste
```

### Configuration constants

```python
DATE_START    = date(2025, 1, 1)
DATE_END      = date(2025, 12, 31)
MEAL_PERIODS  = ["Breakfast", "Lunch", "Dinner", "Snack"]
WASTE_REASONS = ["overproduction", "spoilage", "low demand",
                 "forecast miss", "prep error", "plate waste"]
CATEGORIES    = ["Main Course", "Starter", "Dessert", "Beverage",
                 "Salads", "Dairy", "Fresh Juice", "Snacks", "Breads"]
```

### Post-generation integrity checks

After generating all files, the script runs two self-validation checks and prints results:

```python
# Check 1: No orphan waste rows (all waste keys exist in production)
orphan_waste = waste_keys - prod_keys

# Check 2: No waste > prepared violations
merged = waste_df.merge(prod_df[...], on=[...], how="left")
violations = (merged["quantity_wasted"] > merged["quantity_prepared"]).sum()
```

### Running the generator

```bash
# Default (10,000 production / 6,000 waste)
python ingestion/data_generator.py

# Custom row counts
python ingestion/data_generator.py --rows-production 5000 --rows-waste 3000
```

Output goes to `data/raw/` by default (configurable via `DATA_DIR` env var).

---

## 5. Bronze Layer — Ingestion

**File:** [ingestion/bronze_loader.py](../ingestion/bronze_loader.py)
**Glue job:** `food_waste_bronze` (Python shell, 0.0625 DPU)

### What it does

1. Reads each `macro_*.csv` from local `data/raw/` (dev) or `s3://bucket/raw/` (production)
2. Strips whitespace from all string columns
3. Runs a hard-stop DQ gate (row count + primary key nulls)
4. Adds three metadata columns to every row
5. Serialises to Parquet in-memory and uploads to S3

### Source map

```python
SOURCES = {
    "production_logs": {
        "file": "macro_production_logs.csv",
        "pk": ["date", "location_id", "menu_item_id", "meal_period"],
    },
    "waste_logs": {
        "file": "macro_waste_logs.csv",
        "pk": ["date", "location_id", "menu_item_id", "meal_period"],
    },
    "menu_data":      {"file": "macro_menu_data.csv",     "pk": ["menu_item_id"]},
    "location_data":  {"file": "macro_location_data.csv", "pk": ["location_id"]},
    "supplier_data":  {"file": "macro_supplier_data.csv", "pk": ["supplier_record_id"]},
}
```

### DQ gate (pandas)

```python
def run_bronze_dq(df: pd.DataFrame, source_name: str, pk_cols: list[str]) -> None:
    if len(df) == 0:
        raise ValueError(f"DQ GATE FAILED [{source_name}]: DataFrame is empty.")
    for col in pk_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(
                f"DQ GATE FAILED [{source_name}]: PK column '{col}' has {null_count} null values."
            )
```

### Metadata columns added

| Column | Value |
|--------|-------|
| `ingestion_timestamp` | UTC ISO string at time of run |
| `source_file` | Original CSV filename |
| `batch_id` | UUID shared across all 5 files in one run |

### S3 output structure

```
s3://food-waste-360-596234624522/bronze/
  source=production_logs/date=2025-01-15/data.parquet
  source=waste_logs/date=2025-01-15/data.parquet
  source=menu_data/date=2025-01-15/data.parquet
  source=location_data/date=2025-01-15/data.parquet
  source=supplier_data/date=2025-01-15/data.parquet
```

### CSV reader (local vs S3)

```python
def _read_csv(data_dir: str, filename: str) -> pd.DataFrame:
    if data_dir.startswith("s3://"):
        s3_path = data_dir.rstrip("/") + "/" + filename
        bucket, key = s3_path[5:].split("/", 1)
        resp = boto3.client("s3", region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(resp["Body"].read()), dtype=str)
    else:
        return pd.read_csv(os.path.join(data_dir, filename), dtype=str)
```

All CSVs are read as `dtype=str` to preserve raw values before type-casting occurs in Silver.

---

## 6. Silver Layer — Transform

**File:** [transforms/silver_transform.py](../transforms/silver_transform.py)
**Glue job:** `food_waste_silver` (Spark, 2 × G.1X workers)

### Overview

The Silver job reads all four operational sources from Bronze, cleans and type-casts each DataFrame, joins them, computes four derived analytical columns, runs 10 DQ checks, then writes partitioned Parquet (idempotent).

Supplier data is **not** included in Silver — it is loaded directly from Bronze to Gold via SCD2.

### SparkSession config

```python
def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FoodWaste_SilverTransform")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
```

The `dynamic` partition overwrite mode is critical for idempotency — only the partitions being written are replaced, not the entire table.

### Cleaning functions

#### `clean_production(df)`

```python
def clean_production(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd").cast(DateType()))
        .withColumn("quantity_prepared",      F.col("quantity_prepared").cast(DoubleType()))
        .withColumn("cost_per_unit",          F.col("cost_per_unit").cast(DoubleType()))
        .withColumn("planned_quantity",       F.col("planned_quantity").cast(DoubleType()))
        .withColumn("actual_served_estimate", F.col("actual_served_estimate").cast(DoubleType()))
        .withColumn("location_id",  F.trim(F.col("location_id")))
        .withColumn("menu_item_id", F.trim(F.col("menu_item_id")))
        .withColumn("meal_period",  F.trim(F.lower(F.col("meal_period"))))
        .withColumn("category",     F.trim(F.col("category")))
    )
```

#### `clean_waste(df)`

```python
def clean_waste(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("date",             F.to_date(F.col("date"), "yyyy-MM-dd").cast(DateType()))
        .withColumn("quantity_wasted",    F.col("quantity_wasted").cast(DoubleType()))
        .withColumn("location_id",        F.trim(F.col("location_id")))
        .withColumn("menu_item_id",       F.trim(F.col("menu_item_id")))
        .withColumn("meal_period",        F.trim(F.lower(F.col("meal_period"))))
        .withColumn("waste_reason",       F.trim(F.lower(F.col("waste_reason"))))
        .withColumn("waste_stage",        F.trim(F.col("waste_stage")))
    )
```

#### `clean_menu(df)`

```python
def clean_menu(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("menu_item_id",    F.trim(F.col("menu_item_id")))
        .withColumn("menu_item_name",    F.trim(F.col("menu_item_name")))
        .withColumn("category",          F.upper(F.trim(F.col("category"))))   # UPPERCASED
        .withColumn("sub_category",      F.trim(F.col("sub_category")))
        .withColumn("cost_per_unit",     F.col("cost_per_unit").cast(DoubleType()))
        .withColumn("shelf_life_hours",  F.col("shelf_life_hours").cast(IntegerType()))
    )
```

#### `clean_location(df)`

```python
def clean_location(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("location_id",   F.trim(F.col("location_id")))
        .withColumn("location_name",   F.trim(F.col("location_name")))
        .withColumn("city",            F.trim(F.col("city")))
        .withColumn("region",          F.trim(F.col("region")))
        .dropDuplicates(["location_id"])   # defensive dedup
    )
```

### Join strategy

```python
def build_silver(prod_df, waste_df, menu_df, loc_df) -> DataFrame:
    # Step 1: aggregate waste to production grain (many-waste:one-production)
    waste_agg = waste_df.groupBy(
        "date", "location_id", "menu_item_id", "meal_period"
    ).agg(
        F.sum("quantity_wasted").alias("quantity_wasted"),
        F.first("waste_reason").alias("waste_reason"),
        F.first("waste_stage").alias("waste_stage"),
    )

    # Step 2: menu — drop cost_per_unit to avoid duplicate column error
    # (production already carries cost_per_unit as a denormalised column)
    menu_dedup = menu_df.dropDuplicates(["menu_item_id"]).select(
        "menu_item_id", "sub_category", "veg_flag", "shelf_life_hours", "prep_complexity",
    )

    # Step 3: location — drop location_name (already in production)
    loc_dedup = loc_df.select(
        "location_id", "city", "region", "location_type", "capacity", "storage_rating",
    )

    # Step 4: join chain
    silver = (
        prod_df
        .join(waste_agg, on=["date","location_id","menu_item_id","meal_period"], how="left")
        .join(menu_dedup, on="menu_item_id", how="left")
        .join(loc_dedup,  on="location_id",  how="left")
    )
```

**Why LEFT joins:** Not every production row has a corresponding waste event. LEFT join preserves all production rows; waste columns default to `null` and are coalesced to `0.0`.

### Derived columns

```python
# Coalesce waste to 0 where no waste record exists
silver = silver.withColumn(
    "quantity_wasted", F.coalesce(F.col("quantity_wasted"), F.lit(0.0))
)

# Four analytical columns
silver = (
    silver
    .withColumn("waste_percentage",
        F.when(F.col("quantity_prepared") > 0,
               (F.col("quantity_wasted") / F.col("quantity_prepared")) * 100
        ).otherwise(F.lit(0.0))
    )
    .withColumn("waste_cost",
        F.col("quantity_wasted") * F.col("cost_per_unit")
    )
    .withColumn("quantity_consumed",
        F.col("quantity_prepared") - F.col("quantity_wasted")
    )
    .withColumn("demand_gap",
        F.col("planned_quantity") - F.col("actual_served_estimate")
    )
    # Partition columns
    .withColumn("year",  F.year(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
)
```

| Column | Formula | Meaning |
|--------|---------|---------|
| `waste_percentage` | `(wasted / prepared) * 100` | % of prepared batch that was wasted |
| `waste_cost` | `wasted * cost_per_unit` | INR cost of wasted food |
| `quantity_consumed` | `prepared - wasted` | Actual food served/consumed |
| `demand_gap` | `planned - actual_served` | Over/under-production vs forecast |

### Silver DQ checks (10 hard stops)

```python
VALID_WASTE_REASONS = ["overproduction", "spoilage", "low demand", "plate waste",
                       "forecast miss", "prep error", None]

def silver_dq_checks(df):
    return [
        ("waste_lte_prepared",      F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ("waste_pct_0_to_100",      F.col("waste_percentage").between(0, 100)),
        ("location_id_not_null",    F.col("location_id").isNotNull()),
        ("menu_item_id_not_null",   F.col("menu_item_id").isNotNull()),
        ("date_not_null",           F.col("date").isNotNull()),
        ("valid_waste_reason",      valid_reasons_col),
        ("waste_cost_gte_0",        F.col("waste_cost") >= 0),
        ("quantity_consumed_gte_0", F.col("quantity_consumed") >= 0),
        ("quantity_prepared_gt_0",  F.col("quantity_prepared") > 0),
        ("waste_pct_not_null",      F.col("waste_percentage").isNotNull()),
    ]
```

### Write pattern (idempotent)

```python
out_path = f"s3://{S3_BUCKET}/silver/"
(
    silver.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(out_path)
)
```

With `spark.sql.sources.partitionOverwriteMode=dynamic`, re-running for the same year/month replaces only that partition — no duplicates are created.

---

## 7. Quality Checks — Shared DQ Gate

**File:** [transforms/quality_checks.py](../transforms/quality_checks.py)

A single shared utility used by both Silver and Gold layers.

```python
def run_dq_gate(df: DataFrame, checks: list[tuple[str, Column]]) -> None:
    """
    Hard-stop DQ gate for PySpark DataFrames.
    Each condition evaluates True for VALID rows.
    Raises ValueError if any check finds failing rows.
    """
    for check_name, condition in checks:
        failing = df.filter(~condition).count()
        if failing > 0:
            raise ValueError(f"DQ GATE FAILED: {check_name} — {failing} rows")
        logger.info("DQ PASSED: %s", check_name)
        print(f"  DQ PASSED: {check_name}")
```

**Design principle:** DQ checks are not warnings — they are hard stops. If `run_dq_gate` raises, Airflow marks the task as FAILED and all downstream tasks are blocked.

**Usage pattern:**

```python
# Before writing any layer:
run_dq_gate(silver_df, silver_dq_checks(silver_df))
```

---

## 8. Gold Layer — Dimension Loaders

**File:** [warehouse/dim_loaders.py](../warehouse/dim_loaders.py)
**Part of Glue job:** `food_waste_gold`

Loads 6 of the 7 dimension tables. DIM_SUPPLIER (SCD2) is handled separately.

### DIM_DATE — programmatic generation

```python
def load_dim_date() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", "2026-12-31", freq="D")
    df = pd.DataFrame({
        "date_sk":    dates.strftime("%Y%m%d").astype(int),  # e.g. 20250101
        "full_date":  dates.strftime("%Y-%m-%d"),
        "day_of_week": dates.day_of_week + 1,  # 1=Mon … 7=Sun
        "day_name":   dates.day_name(),
        "month":      dates.month,
        "month_name": dates.month_name(),
        "quarter":    dates.quarter,
        "year":       dates.year,
        "is_weekend": dates.day_of_week >= 5,
    })
```

### DIM_CATEGORY — derived from Silver

```python
def load_dim_category(silver_df: pd.DataFrame) -> pd.DataFrame:
    categories = silver_df["category"].dropna().str.upper().str.strip().unique()
    df = pd.DataFrame({
        "category_sk":   [str(uuid.uuid4()) for _ in categories],
        "category_name": categories,
    })
```

### DIM_MEAL_PERIOD and DIM_WASTE_REASON — static

```python
def load_dim_meal_period() -> pd.DataFrame:
    periods = ["breakfast", "lunch", "dinner", "snack"]
    df = pd.DataFrame({
        "meal_period_sk":   [str(uuid.uuid4()) for _ in periods],
        "meal_period_name": periods,
    })

def load_dim_waste_reason() -> pd.DataFrame:
    reasons = ["overproduction", "spoilage", "low demand",
               "plate waste", "forecast miss", "prep error"]
    df = pd.DataFrame({
        "waste_reason_sk":   [str(uuid.uuid4()) for _ in reasons],
        "waste_reason_name": reasons,
    })
```

### DIM_LOCATION and DIM_MENU — SCD1 upsert

SCD1 means we update in place — no history is preserved. The pattern:

1. Read the incoming data (distinct rows from Silver)
2. Try to read the existing dimension from S3 (empty DataFrame if first run)
3. Left-merge on natural key to preserve existing surrogate keys
4. Assign new UUIDs only for rows with no existing surrogate key

```python
def load_dim_location(silver_df: pd.DataFrame) -> pd.DataFrame:
    src = (
        silver_df[["location_id", "location_name", "city", "region",
                   "location_type", "capacity", "storage_rating"]]
        .drop_duplicates("location_id")
        .reset_index(drop=True)
    )
    try:
        existing = wr.s3.read_parquet(path=f"s3://{S3_BUCKET}/gold/dims/dim_location/data.parquet")
    except Exception:
        existing = pd.DataFrame(columns=["location_sk", "location_id"])

    merged = src.merge(existing[["location_id", "location_sk"]], on="location_id", how="left")
    merged["location_sk"] = merged["location_sk"].apply(
        lambda sk: sk if pd.notna(sk) else str(uuid.uuid4())
    )
```

### DQ gate for all dimensions

Every dimension runs a duplicate-SK check before writing:

```python
def check_no_duplicate_sk(df, sk_col, dim_name):
    dupes = df[sk_col].duplicated().sum()
    if dupes > 0:
        raise ValueError(f"DQ GATE FAILED [{dim_name}]: {dupes} duplicate surrogate keys.")
```

### Dimension load order

```python
def run_dim_loads(silver_df: pd.DataFrame) -> None:
    load_dim_date()           # no dependencies
    load_dim_category(silver_df)
    load_dim_meal_period()
    load_dim_waste_reason()
    load_dim_location(silver_df)
    load_dim_menu(silver_df)
    # DIM_SUPPLIER is called separately (scd2_supplier.py)
```

---

## 9. Gold Layer — SCD2 Supplier Dimension

**File:** [warehouse/scd2_supplier.py](../warehouse/scd2_supplier.py)

### SCD2 overview

Slowly Changing Dimension Type 2 tracks attribute history by creating a new row for every change to a tracked column, instead of overwriting the existing row.

**Tracked columns:** `supplier_name`, `lead_time_days`, `quality_score`

**Required SCD2 columns:**

| Column | Description |
|--------|-------------|
| `supplier_sk` | UUID surrogate key — new UUID per version |
| `supplier_id` | Natural key — stable across versions |
| `effective_date` | ISO date this version became active |
| `expiry_date` | ISO date this version ended (`9999-12-31` = current) |
| `is_current` | `True` for the active version only |

### Core merge logic

```python
def apply_scd2(existing, incoming, today):
    yesterday = today - timedelta(days=1)

    if existing is None or existing.empty:
        # First load: insert all as new current records
        new_rows = incoming.copy()
        new_rows["supplier_sk"]     = [str(uuid.uuid4()) for _ in range(len(new_rows))]
        new_rows["effective_date"]  = today.isoformat()
        new_rows["expiry_date"]     = FAR_FUTURE.isoformat()  # 9999-12-31
        new_rows["is_current"]      = True
        return new_rows[_dim_cols()]

    result = existing.copy()
    for _, new_row in incoming.iterrows():
        sid = new_row["supplier_id"]
        current_rec = result[(result["supplier_id"] == sid) & (result["is_current"])]

        if current_rec.empty:
            # New supplier → insert
            insert = _build_row(new_row, today, FAR_FUTURE, True)
            result = pd.concat([result, pd.DataFrame([insert])], ignore_index=True)
        else:
            idx = current_rec.index[0]
            changed = any(
                str(result.at[idx, c]) != str(new_row.get(c, ""))
                for c in TRACKED_COLS
            )
            if changed:
                # Close old record
                result.at[idx, "expiry_date"] = yesterday.isoformat()
                result.at[idx, "is_current"]  = False
                # Insert new current record
                insert = _build_row(new_row, today, FAR_FUTURE, True)
                result = pd.concat([result, pd.DataFrame([insert])], ignore_index=True)
            # else: no change → no action
```

### SCD2 decision table

| Scenario | Action |
|----------|--------|
| `supplier_id` not in existing | Insert as new current (`is_current=True`, `expiry=9999-12-31`) |
| `supplier_id` exists, tracked col changed | Close old row (`expiry=yesterday`, `is_current=False`), insert new current |
| `supplier_id` exists, no change | No action — leave existing row untouched |

### SCD2 DQ checks (3 hard stops)

```python
def run_scd2_dq(df: pd.DataFrame) -> None:
    # 1. Only one is_current=True per supplier_id
    current_counts = df[df["is_current"]].groupby("supplier_id").size()
    multi = current_counts[current_counts > 1]
    if not multi.empty:
        raise ValueError(f"DQ GATE FAILED [DIM_SUPPLIER]: multiple is_current=True rows ...")

    # 2. All supplier_sk values unique
    dupes = df["supplier_sk"].duplicated().sum()
    if dupes > 0:
        raise ValueError(f"DQ GATE FAILED [DIM_SUPPLIER]: {dupes} duplicate supplier_sk values.")

    # 3. No overlapping date ranges per supplier_id
    for sid, grp in df.groupby("supplier_id"):
        grp_sorted = grp.sort_values("effective_date")
        for i in range(1, len(grp_sorted)):
            if prev_expiry >= curr_effective:
                raise ValueError(f"DQ GATE FAILED: overlapping date ranges for supplier_id={sid}")
```

### Source: Bronze, not Silver

Supplier data is loaded from Bronze parquet directly, not from Silver, because Silver does not join supplier data:

```python
path = f"s3://{S3_BUCKET}/bronze/source=supplier_data/"
supplier_raw = wr.s3.read_parquet(path=path, dataset=True)
```

---

## 10. Gold Layer — Fact Loaders

**File:** [warehouse/fact_loaders.py](../warehouse/fact_loaders.py)

Loads 4 fact tables by resolving dimension surrogate keys from the Silver layer.

### SK resolution pattern (same for all facts)

```python
# Convert date to string for join with DIM_DATE.full_date
df["full_date"] = df["date"].astype(str)

# Resolve all four SKs
df = df.merge(dim_date[["full_date", "date_sk"]],            on="full_date",    how="left")
df = df.merge(dim_location[["location_id", "location_sk"]],  on="location_id",  how="left")
df = df.merge(dim_menu[["menu_item_id", "menu_sk"]],          on="menu_item_id", how="left")
dim_mp = dim_meal_period.rename(columns={"meal_period_name": "meal_period"})
df = df.merge(dim_mp[["meal_period", "meal_period_sk"]],      on="meal_period",  how="left")
```

### FACT_PRODUCTION

Grain: one row per (date, location, menu item, meal period).

```python
def load_fact_production(silver, dim_date, dim_location, dim_menu, dim_meal_period):
    df = silver[["date", "location_id", "menu_item_id", "meal_period",
                 "quantity_prepared", "cost_per_unit", "batch_id", "year", "month"]].copy()
    # ... SK resolution ...
    df["production_sk"] = [str(uuid.uuid4()) for _ in range(len(df))]

    _pandas_dq(result, [
        ("date_sk_not_null",        result["date_sk"].notna()),
        ("location_sk_not_null",    result["location_sk"].notna()),
        ("menu_sk_not_null",        result["menu_sk"].notna()),
        ("meal_period_sk_not_null", result["meal_period_sk"].notna()),
    ], "FACT_PRODUCTION")
```

### FACT_WASTE

Only rows where `waste_reason` is not null (i.e. actual waste events) are included.

```python
def load_fact_waste(silver, dim_date, dim_location, dim_menu,
                    dim_meal_period, dim_waste_reason, dim_supplier):
    df = silver[waste_cols].dropna(subset=["waste_reason"]).copy()
    # ... SK resolution for all dims ...
    # Supplier: join on menu_item_id using current records only
    current_sup = dim_supplier[dim_supplier["is_current"]].copy()
    df = df.merge(current_sup[["menu_item_id", "supplier_sk"]], on="menu_item_id", how="left")
```

### FACT_CONSUMPTION

Derived fact — production volume vs waste vs consumption.

```python
def load_fact_consumption(silver, dim_date, dim_location, dim_menu):
    cols = ["date", "location_id", "menu_item_id", "quantity_prepared",
            "quantity_wasted", "quantity_consumed", "demand_gap", "year", "month"]
    # DQ: consumed >= 0, date_sk not null
```

### FACT_WASTE_SUMMARY

Pre-aggregated table (by location + category + year + month) used by all dashboard queries.

```python
def load_fact_waste_summary(silver, dim_location, dim_category):
    # Only rows where waste actually occurred
    df = silver[silver["quantity_wasted"] > 0].copy()

    agg = (
        df.groupby(["location_id", "category_upper", "year", "month"],
                   as_index=False, observed=True)
        .agg(
            total_waste_quantity=("quantity_wasted", "sum"),
            total_waste_cost=    ("waste_cost",      "sum"),
            avg_waste_percentage=("waste_percentage", "mean"),
            waste_event_count=   ("quantity_wasted", "count"),
        )
    )
```

**Why this table exists:** All dashboard queries hit `FACT_WASTE_SUMMARY` instead of the raw `FACT_WASTE`. Since it is pre-aggregated to location+category+month granularity, it scans orders of magnitude less data per Athena query, staying well within the 1 GB scan limit.

### Pandas DQ gate (mirrors PySpark version)

```python
def _pandas_dq(df: pd.DataFrame, checks: list[tuple], fact_name: str) -> None:
    for check_name, mask in checks:
        failing = (~mask).sum()
        if failing > 0:
            raise ValueError(f"DQ GATE FAILED [{fact_name}] {check_name} — {failing} rows")
```

---

## 11. Athena Analytics

**File:** [analytics/athena_queries.sql](../analytics/athena_queries.sql)
**Workgroup:** `food-waste-wg` (1 GB per-query scan limit)
**Database:** `food_waste_db`

All queries filter on partition columns (`year`, `month`) first, and never use `SELECT *`.

### Query 1 — Monthly waste trend with MoM change (LAG)

```sql
SELECT
    dl.location_name,
    fws.year,
    fws.month,
    fws.total_waste_cost,
    LAG(fws.total_waste_cost) OVER (
        PARTITION BY fws.location_sk
        ORDER BY fws.year, fws.month
    ) AS prev_month_cost,
    ROUND(
        (fws.total_waste_cost - LAG(fws.total_waste_cost) OVER (
            PARTITION BY fws.location_sk ORDER BY fws.year, fws.month
        )) * 100.0
        / NULLIF(LAG(fws.total_waste_cost) OVER (
            PARTITION BY fws.location_sk ORDER BY fws.year, fws.month
        ), 0),
        2
    ) AS mom_pct_change
FROM food_waste_db.fact_waste_summary fws
JOIN food_waste_db.dim_location dl ON fws.location_sk = dl.location_sk
WHERE fws.year = 2025 AND fws.month BETWEEN 1 AND 12
ORDER BY dl.location_name, fws.year, fws.month;
```

**Answers:** Which locations are improving or worsening each month?
**Technique:** LAG window function

### Query 2 — Top 10 menu items by waste cost (DENSE_RANK)

```sql
SELECT ranked.menu_item_name, ranked.category,
       ranked.total_waste_cost, ranked.waste_rank
FROM (
    SELECT
        dm.menu_item_name,
        dm.category,
        SUM(fw.waste_cost)                              AS total_waste_cost,
        DENSE_RANK() OVER (ORDER BY SUM(fw.waste_cost) DESC) AS waste_rank
    FROM food_waste_db.fact_waste fw
    JOIN food_waste_db.dim_menu dm ON fw.menu_sk = dm.menu_sk
    WHERE fw.year = 2025 AND fw.month BETWEEN 1 AND 12
    GROUP BY dm.menu_item_name, dm.category
) ranked
WHERE ranked.waste_rank <= 10
ORDER BY ranked.waste_rank;
```

**Answers:** Which menu items are costing the most in waste?
**Technique:** DENSE_RANK (allows ties — multiple items can share the same rank)

### Query 3 — Demand vs supply gap by category (CTE)

```sql
WITH category_supply AS (
    SELECT dm.category,
        SUM(fp.quantity_prepared) AS total_prepared,
        SUM(fc.quantity_consumed) AS total_consumed,
        SUM(fc.demand_gap)        AS total_demand_gap,
        COUNT(fp.production_sk)   AS production_events
    FROM food_waste_db.fact_production fp
    JOIN food_waste_db.dim_menu dm ON fp.menu_sk = dm.menu_sk
    JOIN food_waste_db.fact_consumption fc
        ON fp.date_sk = fc.date_sk
       AND fp.location_sk = fc.location_sk
       AND fp.menu_sk = fc.menu_sk
    WHERE fp.year = 2025 AND fp.month BETWEEN 1 AND 12
    GROUP BY dm.category
),
category_waste AS (
    SELECT fws.category,
        SUM(fws.total_waste_cost)     AS total_waste_cost,
        AVG(fws.avg_waste_percentage) AS avg_waste_pct
    FROM food_waste_db.fact_waste_summary fws
    WHERE fws.year = 2025 AND fws.month BETWEEN 1 AND 12
    GROUP BY fws.category
)
SELECT cs.category, cs.total_prepared, cs.total_consumed, cs.total_demand_gap,
       cw.total_waste_cost, ROUND(cw.avg_waste_pct, 2) AS avg_waste_pct,
       ROUND(cs.total_demand_gap * 100.0 / NULLIF(cs.total_prepared, 0), 2) AS gap_pct
FROM category_supply cs
JOIN category_waste cw ON cs.category = cw.category
ORDER BY cs.total_demand_gap DESC;
```

**Answers:** Which categories consistently over-prepare?
**Technique:** CTE (two CTEs merged in final SELECT)

### Query 4 — Supplier quality vs waste percentage (window AVG)

```sql
SELECT
    ds.supplier_name, ds.supplier_city, ds.quality_score,
    dm.menu_item_name, dm.category,
    fw.waste_percentage,
    AVG(fw.waste_percentage) OVER (PARTITION BY fw.supplier_sk) AS supplier_avg_waste_pct,
    AVG(fw.waste_percentage) OVER ()                            AS overall_avg_waste_pct
FROM food_waste_db.fact_waste fw
JOIN food_waste_db.dim_supplier ds ON fw.supplier_sk = ds.supplier_sk
JOIN food_waste_db.dim_menu dm     ON fw.menu_sk = dm.menu_sk
WHERE fw.year = 2025 AND fw.month BETWEEN 1 AND 12
  AND ds.is_current = true
ORDER BY ds.quality_score ASC, fw.waste_percentage DESC;
```

**Answers:** Do lower-quality suppliers correlate with higher waste?
**Technique:** Window AVG over supplier partition + global AVG

### Query 5 — Waste reason breakdown by meal period (SUM OVER)

```sql
SELECT
    dmp.meal_period_name,
    dwr.waste_reason_name,
    COUNT(fw.waste_sk)  AS event_count,
    SUM(fw.waste_cost)  AS total_cost,
    ROUND(
        SUM(fw.waste_cost) * 100.0
        / NULLIF(SUM(SUM(fw.waste_cost)) OVER (PARTITION BY fw.year, fw.month), 0),
        2
    ) AS pct_of_monthly_total
FROM food_waste_db.fact_waste fw
JOIN food_waste_db.dim_waste_reason dwr ON fw.waste_reason_sk = dwr.waste_reason_sk
JOIN food_waste_db.dim_meal_period dmp  ON fw.year = 2025
WHERE fw.year = 2025 AND fw.month BETWEEN 1 AND 12
GROUP BY dmp.meal_period_name, dwr.waste_reason_name, fw.year, fw.month
ORDER BY dmp.meal_period_name, total_cost DESC;
```

**Answers:** Which meal periods have the worst waste patterns?
**Technique:** Nested window SUM (`SUM(SUM(...)) OVER`) for percentage-of-total

---

## 12. Root Cause Classification View

**File:** [analytics/root_cause_view.sql](../analytics/root_cause_view.sql)

A virtual table (Athena view) that classifies each location+category+month group into one of four root causes using rule-based CASE logic.

```sql
CREATE OR REPLACE VIEW food_waste_db.waste_root_cause AS
SELECT
    dl.location_name, dl.city, dl.region,
    fws.category AS category_name,
    fws.year, fws.month,
    ROUND(fws.avg_waste_percentage, 2)  AS waste_percentage,
    fws.total_waste_cost,
    fws.total_waste_quantity,
    fws.waste_event_count,

    CASE
        WHEN fws.avg_waste_percentage > 40
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) > 5
            THEN 'Overproduction'

        WHEN fws.category IN ('SALADS','DAIRY','FRESH JUICE','FRUITS',...)
             AND fws.avg_waste_percentage > 20
            THEN 'Storage / Spoilage'

        WHEN fws.avg_waste_percentage > 30
             AND (fws.total_waste_quantity / NULLIF(fws.waste_event_count, 0)) <= 5
            THEN 'Portion Mismatch'

        ELSE 'Low Demand'
    END AS root_cause,

    CASE ... END AS recommendation
FROM food_waste_db.fact_waste_summary fws
JOIN food_waste_db.dim_location dl ON fws.location_sk = dl.location_sk
WHERE fws.year = 2025 AND fws.month BETWEEN 1 AND 12
```

### Classification rules

| Root Cause | Condition | Recommendation |
|------------|-----------|----------------|
| **Overproduction** | `avg_waste_pct > 40` AND `volume_per_event > 5` | Reduce batch size by 20-30% |
| **Storage / Spoilage** | Perishable category AND `avg_waste_pct > 20` | Review storage + supplier lead time |
| **Portion Mismatch** | `avg_waste_pct > 30` AND `volume_per_event <= 5` | Adjust portion sizes |
| **Low Demand** | All other cases | Review menu item demand |

**Perishable categories proxy:** `SALADS`, `DAIRY`, `FRESH JUICE`, `FRUITS` (high spoilage risk if stored improperly).

**Volume-per-event proxy for demand_gap:** `total_waste_quantity / waste_event_count` — high ratio = large batches wasted = overproduction signal.

---

## 13. Airflow Orchestration

**File:** [orchestration/dags/food_waste_pipeline.py](../orchestration/dags/food_waste_pipeline.py)
**Tool:** Apache Airflow via Astro CLI (local Docker)

### DAG structure

```python
with DAG(
    dag_id="food_waste_pipeline",
    schedule=None,           # manual trigger only — no automated runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "food-waste-360",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
) as dag:

    bronze_ingestion = GlueJobOperator(
        task_id="bronze_ingestion",
        job_name="food_waste_bronze",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
    )

    silver_transform = GlueJobOperator(
        task_id="silver_transform",
        job_name="food_waste_silver",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
    )

    gold_load = GlueJobOperator(
        task_id="gold_load",
        job_name="food_waste_gold",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
    )

    bronze_ingestion >> silver_transform >> gold_load
```

### Task dependency chain

```
bronze_ingestion → silver_transform → gold_load
```

`wait_for_completion=True` means each GlueJobOperator polls the Glue API until the job reaches `SUCCEEDED` or `FAILED`. If a Glue job fails (including DQ gate failures), the Airflow task raises and all downstream tasks are blocked.

### Failure callback

```python
def on_failure_callback(context):
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    logger.error("Task FAILED | task_id=%s | execution_date=%s | dag_id=%s",
                 task_id, execution_date, context.get("dag").dag_id)
```

### AWS connection setup

Credentials are stored in Airflow's connection manager — **never in code**:

```
Admin > Connections > New Connection
  Conn Id:   aws_default
  Conn Type: Amazon Web Services
  Extra:     {"aws_access_key_id": "...",
               "aws_secret_access_key": "...",
               "region_name": "ap-south-1"}
```

### Starting the Airflow environment

```bash
cd orchestration
astro dev start
# Open http://localhost:8080  (admin / admin)
# Add aws_default connection in UI
# Then trigger the DAG
astro dev run dags trigger food_waste_pipeline
```

---

## 14. AWS Infrastructure Setup

### Run order

```bash
python aws_setup/01_s3_setup.py     # Create S3 buckets + folder structure + lifecycle rules
python aws_setup/02_iam_setup.py    # IAM role + managed + inline policies
python setup_aws.py                  # Upload Glue scripts + register Glue jobs + Athena setup
```

All scripts are idempotent — safe to re-run.

### Running the pipeline manually (without Airflow)

```bash
export S3_BUCKET=food-waste-360-596234624522
export AWS_REGION=ap-south-1

# 1. Bronze
aws glue start-job-run --job-name food_waste_bronze \
  --arguments '{"--S3_BUCKET":"food-waste-360-596234624522","--AWS_REGION":"ap-south-1"}' \
  --region ap-south-1

# 2. Silver (after Bronze SUCCEEDED)
aws glue start-job-run --job-name food_waste_silver \
  --arguments '{"--S3_BUCKET":"food-waste-360-596234624522","--AWS_REGION":"ap-south-1"}' \
  --region ap-south-1

# 3. Gold (after Silver SUCCEEDED)
aws glue start-job-run --job-name food_waste_gold \
  --arguments '{"--S3_BUCKET":"food-waste-360-596234624522","--AWS_REGION":"ap-south-1"}' \
  --region ap-south-1

# 4. Load Athena partitions (run once in Athena console, food-waste-wg workgroup)
MSCK REPAIR TABLE food_waste_db.fact_production;
MSCK REPAIR TABLE food_waste_db.fact_waste;
MSCK REPAIR TABLE food_waste_db.fact_consumption;
MSCK REPAIR TABLE food_waste_db.fact_waste_summary;

# 5. Create root cause view (paste analytics/root_cause_view.sql in Athena console)

# 6. Launch dashboard
streamlit run dashboard/app.py
```

### Polling Glue job status

```bash
aws glue get-job-runs --job-name food_waste_silver --region ap-south-1 \
  --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}'
```

---

## 15. CI/CD — GitHub Actions

**File:** [.github/workflows/pipeline_tests.yml](../.github/workflows/pipeline_tests.yml)

### Workflow triggers

```yaml
on:
  push:
    branches: ["**"]
  pull_request:
    branches: ["**"]
```

Runs on every push and every pull request, across all branches.

### Jobs

**Job 1: Run tests**

```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: "3.11"

- name: Install dependencies
  run: pip install -r requirements.txt

- name: Run tests
  run: pytest tests/ -v
```

Tests run against in-memory PySpark DataFrames — no AWS calls, no credentials required.

**Job 2: Credential scan**

```yaml
- name: Scan for hardcoded AWS credentials
  run: |
    if grep -r "AKIA[0-9A-Z]{16}" . --include="*.py" --include="*.sql" \
       --include="*.yml" --exclude-dir=".git"; then
      echo "ERROR: Hardcoded AWS access key found!"
      exit 1
    fi
    echo "Credential scan passed — no hardcoded keys found."
```

Scans all Python, SQL, and YAML files for the `AKIA...` access key pattern. If any match is found, the workflow fails and the commit is blocked.

---

## 16. Test Suite

**Directory:** [tests/](../tests/)
**Framework:** pytest
**Approach:** All tests use in-memory PySpark or pandas DataFrames — no AWS calls, no network required.

### test_quality_checks.py

Tests the shared `run_dq_gate()` utility with 6 test cases.

#### Test 1: Null check raises

```python
def test_null_check_fails(spark):
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", None, 80.0, None)],
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("quantity_wasted_not_null", F.col("quantity_wasted").isNotNull()),
        ])
```

#### Test 2: Range check raises

```python
def test_range_check_fails(spark):
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 90.0, 80.0, 112.5)],  # 112.5 > 100
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [("waste_pct_0_to_100", F.col("waste_pct").between(0, 100))])
```

#### Test 3: waste > prepared raises

```python
def test_waste_lte_prepared_fails(spark):
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 110.0, 100.0, 110.0)],  # 110 > 100
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("waste_lte_prepared", F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ])
```

#### Test 4: All checks pass silently

```python
def test_all_checks_pass(spark):
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 20.0, 80.0, 25.0)],
        schema=SCHEMA,
    )
    run_dq_gate(df, [
        ("not_null",       F.col("quantity_wasted").isNotNull()),
        ("pct_range",      F.col("waste_pct").between(0, 100)),
        ("waste_lte_prep", F.col("quantity_wasted") <= F.col("quantity_prepared")),
    ])
    # No exception = test passes
```

#### Test 5: Empty checks list never raises

```python
def test_empty_checks_list_passes(spark):
    df = spark.createDataFrame([("X", 5.0, 10.0, 50.0)], schema=SCHEMA)
    run_dq_gate(df, [])
```

---

### test_silver_transforms.py

Tests `build_silver()` derived column calculations and DQ gate enforcement. Uses a helper to build Silver DataFrames from minimal in-memory data.

```python
def make_silver(spark, prod_rows, waste_rows=None):
    prod  = spark.createDataFrame(prod_rows or [], schema=PROD_SCHEMA)
    waste = spark.createDataFrame(waste_rows or [], schema=WASTE_SCHEMA)
    menu  = spark.createDataFrame([("MI001","Dal Rice","MAIN COURSE","sub","Yes",67.0,24,"Low")], MENU_SCHEMA)
    loc   = spark.createDataFrame([("LOC001","Campus Cafe","Bangalore","South","Campus","500","A")], LOC_SCHEMA)
    return build_silver(prod, waste, menu, loc)
```

#### Test 1: waste_percentage correct

```python
def test_waste_percentage_calculation(spark):
    prod_rows  = [(date(2025,1,1), "LOC001", "MI001", "lunch", 100.0, 50.0, 110.0, 90.0, "b1")]
    waste_rows = [(date(2025,1,1), "LOC001", "MI001", "lunch", 20.0, "overproduction", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    row = silver.select("waste_percentage").first()
    assert abs(row["waste_percentage"] - 20.0) < 0.01   # 20/100 * 100 = 20.0
```

#### Test 2: waste_cost correct

```python
def test_waste_cost_calculation(spark):
    # 20 units wasted * 50 cost_per_unit = 1000
    row = silver.select("waste_cost").first()
    assert abs(row["waste_cost"] - 1000.0) < 0.01
```

#### Test 3: quantity_consumed correct

```python
def test_quantity_consumed_calculation(spark):
    # 100 prepared - 30 wasted = 70 consumed
    row = silver.select("quantity_consumed").first()
    assert abs(row["quantity_consumed"] - 70.0) < 0.01
```

#### Test 4: DQ fails when waste > prepared

```python
def test_dq_fails_when_waste_exceeds_prepared(spark):
    prod_rows  = [(date(2025,1,1), "LOC001", "MI001", "lunch", 50.0, 50.0, 60.0, 40.0, "b1")]
    waste_rows = [(date(2025,1,1), "LOC001", "MI001", "lunch", 80.0, "overproduction", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(silver, [
            ("waste_lte_prepared", F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ])
```

#### Test 5: All Silver DQ checks pass for valid row

```python
def test_dq_passes_for_valid_row(spark):
    # 10 wasted out of 100 prepared, all fields valid
    run_dq_gate(silver, [
        ("waste_lte_prepared",     F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ("waste_pct_0_to_100",     F.col("waste_percentage").between(0, 100)),
        ("location_id_not_null",   F.col("location_id").isNotNull()),
        ("quantity_consumed_gte_0",F.col("quantity_consumed") >= 0),
    ])
    # No exception = pass
```

---

### test_scd2.py

Tests `apply_scd2()` merge logic and `run_scd2_dq()` checks using pandas DataFrames.

```python
TODAY     = date(2025, 6, 1)
YESTERDAY = date(2025, 5, 31)
FAR_FUTURE = date(9999, 12, 31)
```

#### Test 1: New supplier inserted as current

```python
def test_new_supplier_inserted_as_current():
    result = apply_scd2(None, make_incoming(), TODAY)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["is_current"] == True
    assert row["expiry_date"] == FAR_FUTURE.isoformat()
    assert row["effective_date"] == TODAY.isoformat()
```

#### Test 2: Attribute change creates new version

```python
def test_changed_attribute_creates_new_version():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=3.8)   # quality_score changed
    result = apply_scd2(existing, incoming, TODAY)

    assert len(result) == 2   # old row closed + new row inserted

    old = result[result["supplier_sk"] == "SK-EXISTING"].iloc[0]
    assert old["is_current"] == False
    assert old["expiry_date"] == YESTERDAY.isoformat()

    new = result[result["supplier_sk"] != "SK-EXISTING"].iloc[0]
    assert new["is_current"] == True
    assert float(new["quality_score"]) == pytest.approx(3.8)
```

#### Test 3: No change → no new row

```python
def test_no_change_produces_no_new_row():
    existing = make_existing(quality_score=4.5)
    incoming = make_incoming(quality_score=4.5)   # identical
    result = apply_scd2(existing, incoming, TODAY)
    assert len(result) == 1
    assert result.iloc[0]["supplier_sk"] == "SK-EXISTING"
```

#### Test 4: Only one is_current=True per supplier_id

```python
def test_only_one_current_per_supplier_after_change():
    result = apply_scd2(make_existing(quality_score=4.5), make_incoming(quality_score=2.0), TODAY)
    current_count = result[result["is_current"] == True].shape[0]
    assert current_count == 1
```

#### Test 5: DQ raises on multiple is_current records

```python
def test_dq_raises_on_multiple_current_records():
    bad = pd.DataFrame([
        {"supplier_sk":"SK1","supplier_id":"SUP001","is_current":True,...},
        {"supplier_sk":"SK2","supplier_id":"SUP001","is_current":True,...},  # duplicate!
    ])
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_scd2_dq(bad)
```

#### Test 6: DQ raises on duplicate supplier_sk

```python
def test_dq_raises_on_duplicate_supplier_sk():
    bad = pd.DataFrame([
        {"supplier_sk":"SK1","supplier_id":"SUP001","is_current":False,...},
        {"supplier_sk":"SK1","supplier_id":"SUP001","is_current":True,...},  # same SK!
    ])
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_scd2_dq(bad)
```

### Running tests

```bash
# All tests
pytest tests/ -v

# Single file
pytest tests/test_scd2.py -v

# With coverage
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## 17. Dashboard

**Directory:** [dashboard/](../dashboard/)
**Tool:** Streamlit + PyAthena
**Start:** `streamlit run dashboard/app.py`

The dashboard has 5 pages. All queries target `FACT_WASTE_SUMMARY` (pre-aggregated) to minimise Athena scan cost.

| Page | File | Focus |
|------|------|-------|
| Overview | `pages/1_overview.py` | Total waste KPIs, monthly trend |
| Location | `pages/2_location.py` | Waste by location, map view |
| Category | `pages/3_category.py` | Waste by food category |
| Trends | `pages/4_trends.py` | MoM change, seasonal patterns |
| Root Cause | `pages/5_root_cause.py` | Root cause classification from `waste_root_cause` view |

---

## 18. Makefile Commands

```makefile
make generate    # python ingestion/data_generator.py
make run         # trigger Airflow DAG via astro dev run
make dashboard   # streamlit run dashboard/app.py
make test        # pytest tests/ -v
```

---

## 19. Cost Profile

All compute is serverless — **zero idle cost** beyond S3 storage.

| Component | Cost at rest | Cost per run |
|-----------|-------------|-------------|
| S3 (~1 GB) | ~$0.02/month | — |
| Glue Bronze (Python shell, 0.0625 DPU) | $0 | ~$0.01 |
| Glue Silver (Spark 2×G.1X, ~5 min) | $0 | ~$0.14 |
| Glue Gold (Python shell, 0.0625 DPU) | $0 | ~$0.01 |
| Athena queries (< 1 GB scan each) | $0 | ~$0.00 |
| Airflow (local Docker) | $0 | $0 |
| **Total** | **~$0.02/month** | **~$0.16/run** |

**Cost controls:**
- Glue job timeout: 30 minutes (hard limit — prevents runaway charges)
- Athena workgroup scan limit: 1 GB per query
- Dashboard queries hit `FACT_WASTE_SUMMARY` not raw `FACT_WASTE` (orders of magnitude less data)

---

## 20. Known Bugs Fixed

### Bug 1 — Silver ambiguous column reference

**Symptom:** `AnalysisException: Reference 'cost_per_unit' is ambiguous`

**Root cause:** Both `prod_df` and `menu_dedup` carried a `cost_per_unit` column. After the join, Spark could not determine which to use.

**Fix:** Removed `cost_per_unit` from the `menu_dedup` select in `build_silver()`. Production's `cost_per_unit` is used for all downstream calculations.

```python
# Before (broken)
menu_dedup = menu_df.dropDuplicates(["menu_item_id"]).select(
    "menu_item_id", "cost_per_unit", "sub_category", ...
)

# After (fixed)
menu_dedup = menu_df.dropDuplicates(["menu_item_id"]).select(
    "menu_item_id", "sub_category", "veg_flag", "shelf_life_hours", "prep_complexity",
)
```

### Bug 2 — Athena DDL duplicate partition columns

**Symptom:** Hive rejected fact table DDL at `MSCK REPAIR TABLE`.

**Root cause:** `create_tables.sql` listed `year INT` and `month INT` in both the column body AND the `PARTITIONED BY` clause. Hive rejects duplicate partition columns.

**Fix:** Removed `year` and `month` from the column body of all four fact tables. All fact tables were dropped and re-registered in Athena.

### Bug 3 — setup_aws.py Tags in glue.update_job()

**Symptom:** `ParamValidationError` on re-runs of `setup_aws.py`.

**Root cause:** `glue.update_job()` does not accept a `Tags` key in `JobUpdate` (only `create_job` accepts Tags).

**Fix:** `update_config` now excludes both `"Name"` and `"Tags"` keys before calling `update_job`.

### Bug 4 — Athena workgroup idempotency string mismatch

**Symptom:** `[FAIL]` printed on re-runs even though workgroup already existed.

**Root cause:** The already-exists error message is `"WorkGroup is already created"` not `"already exists"`. The string match failed silently.

**Fix:** Added `"already created"` as a second match string in the exception handler.
