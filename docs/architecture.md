# Architecture — Food Waste Optimization 360

## Overview

Food Waste Optimization 360 is a batch data engineering platform that ingests operational kitchen data from five CSV sources, processes it through a three-layer medallion architecture, models it into a star schema data warehouse, and surfaces insights via SQL analytics and a Streamlit dashboard.

All compute is serverless (AWS Glue, AWS Athena). The orchestration layer runs locally via Astro CLI (Docker). There is zero idle infrastructure cost — S3 storage (~$0.023/GB/month) is the only ongoing expense.

---

## High-Level Data Flow

```
[Raw CSVs — 5 sources]
  macro_production_logs.csv
  macro_waste_logs.csv
  macro_menu_data.csv
  macro_location_data.csv
  macro_supplier_data.csv
         │
         ▼  ingestion/bronze_loader.py  (boto3/pandas, runs as Glue Python shell)
[S3 Bronze Layer]
  s3://bucket/bronze/source={name}/date={YYYY-MM-DD}/data.parquet
         │
         ▼  transforms/silver_transform.py  (PySpark, runs as Glue Spark job)
[S3 Silver Layer]
  s3://bucket/silver/year={Y}/month={M}/
         │
         ├──▶ warehouse/dim_loaders.py     (pandas, runs as Glue Python shell)
         ├──▶ warehouse/scd2_supplier.py   (pandas, Bronze supplier source)
         └──▶ warehouse/fact_loaders.py    (pandas)
                    │
                    ▼
[S3 Gold Layer — Star Schema]
  s3://bucket/gold/dims/{dim_name}/data.parquet
  s3://bucket/gold/facts/{fact_name}/data.parquet
         │
         ▼  Athena external tables (registered in food_waste_db)
[AWS Athena]
  Analytical queries + waste_root_cause view
         │
         ▼  dashboard/app.py  (PyAthena)
[Streamlit Dashboard — 5 pages]
```

---

## Layer Specifications

### Bronze Layer

**Purpose:** Land raw data on S3 as Parquet with metadata. No business logic applied.

**Script:** `ingestion/bronze_loader.py`
**Glue job:** `food_waste_bronze` (Python shell, 0.0625 DPU)

**Processing steps:**
1. Read each `macro_*.csv` from `DATA_DIR` using pandas
2. Strip whitespace from all string columns
3. Run DQ gates (row count > 0, primary key columns not null) — raises `ValueError` on failure
4. Add three metadata columns to every row:
   - `ingestion_timestamp` — UTC ISO string
   - `source_file` — original filename
   - `batch_id` — UUID shared across all 5 files in the same run
5. Serialize to Parquet in-memory (pyarrow engine)
6. Upload via boto3 to `s3://{S3_BUCKET}/bronze/source={name}/date={today}/data.parquet`

**S3 structure:**
```
bronze/
  source=production_logs/date=2025-01-15/data.parquet
  source=waste_logs/date=2025-01-15/data.parquet
  source=menu_data/date=2025-01-15/data.parquet
  source=location_data/date=2025-01-15/data.parquet
  source=supplier_data/date=2025-01-15/data.parquet
```

**DQ gates (hard stops):**
- Row count > 0
- Composite PK columns not null (date + location_id + menu_item_id + meal_period for logs)

---

### Silver Layer

**Purpose:** Clean, standardise, join all 4 operational sources (production + waste + menu + location), and compute 4 derived analytical columns. Supplier data stays in Bronze — it is loaded separately to Gold via SCD2.

**Script:** `transforms/silver_transform.py`
**Glue job:** `food_waste_silver` (Spark, 2 × G.1X workers)
**Shared utility:** `transforms/quality_checks.py` — `run_dq_gate()`

**Cleaning functions (one per source):**

| Function | Operations |
|----------|-----------|
| `clean_production` | Cast date → DateType, quantities → DoubleType, trim strings, lowercase meal_period |
| `clean_waste` | Cast date + quantity, strip + lowercase waste_reason |
| `clean_menu` | Uppercase category, trim all strings, cast numeric columns |
| `clean_location` | Trim strings, `dropDuplicates(["location_id"])` |
| `clean_supplier` | Trim strings, cast types, parse effective_from date |

**Join strategy:**
```
production
  LEFT JOIN waste_agg   ON (date, location_id, menu_item_id, meal_period)
  LEFT JOIN menu_dedup  ON menu_item_id
  LEFT JOIN location    ON location_id
```
Waste is aggregated to production grain before joining (many waste rows can exist per production row).

**Derived columns:**

| Column | Formula |
|--------|---------|
| `waste_percentage` | `(quantity_wasted / quantity_prepared) * 100` |
| `waste_cost` | `quantity_wasted * cost_per_unit` |
| `quantity_consumed` | `quantity_prepared - quantity_wasted` |
| `demand_gap` | `planned_quantity - actual_served_estimate` |

**DQ gates (10 checks — all hard stops):**
1. `waste_quantity <= prepared_quantity`
2. `waste_percentage` between 0 and 100
3. `location_id` not null
4. `menu_item_id` not null
5. `date` not null
6. Valid `waste_reason` values (normalised set)
7. `waste_cost >= 0`
8. `quantity_consumed >= 0`
9. `quantity_prepared > 0`
10. `waste_percentage` not null

**Write pattern:** `mode("overwrite").partitionBy("year","month")` with `spark.sql.sources.partitionOverwriteMode=dynamic`. Re-running for the same month replaces only that partition — no duplicates.

---

### Gold Layer — Star Schema

**Purpose:** Warehouse-grade dimensional model. Pre-aggregated summary table for low-cost dashboard queries.

**Scripts:**
- `warehouse/dim_loaders.py` — 6 SCD1 + static dimensions
- `warehouse/scd2_supplier.py` — DIM_SUPPLIER with full version history
- `warehouse/fact_loaders.py` — 4 fact tables

**Glue job:** `food_waste_gold` (Python shell, 0.0625 DPU)

#### Dimension tables

| Table | Type | Source | SK column |
|-------|------|--------|-----------|
| DIM_DATE | Static/generated | Programmatic (2025-01-01 to 2025-12-31) | `date_sk` (YYYYMMDD int) |
| DIM_CATEGORY | Static | Distinct categories from Silver | `category_sk` (UUID) |
| DIM_MEAL_PERIOD | Static | Hardcoded 4 values | `meal_period_sk` (UUID) |
| DIM_WASTE_REASON | Static | Hardcoded 6 normalised values | `waste_reason_sk` (UUID) |
| DIM_LOCATION | SCD1 | Silver location columns | `location_sk` (UUID) |
| DIM_MENU | SCD1 | Silver menu columns | `menu_sk` (UUID) |
| DIM_SUPPLIER | SCD2 | Bronze supplier_data (not Silver) | `supplier_sk` (UUID) |

**DIM_SUPPLIER SCD2 logic:**
- Tracked columns: `supplier_name`, `lead_time_days`, `quality_score`
- New supplier_id → insert as current (expiry=9999-12-31, is_current=True)
- Tracked column changed → close old record (expiry=yesterday, is_current=False), insert new current
- No change → no action
- DQ gate: one `is_current=True` per `supplier_id`, no duplicate `supplier_sk`, no overlapping date ranges

**Important:** Supplier data is read directly from Bronze (`s3://bucket/bronze/source=supplier_data/`), not from Silver. Silver does not join supplier data.

#### Fact tables

| Table | Grain | Key measures |
|-------|-------|-------------|
| FACT_PRODUCTION | One row per (date, location, menu, meal_period) | `quantity_prepared`, `cost_per_unit` |
| FACT_WASTE | One row per waste event (same grain as production) | `quantity_wasted`, `waste_percentage`, `waste_cost` |
| FACT_CONSUMPTION | Derived from production+waste | `quantity_consumed`, `demand_gap` |
| FACT_WASTE_SUMMARY | Aggregated by location + category + year + month | `total_waste_quantity`, `total_waste_cost`, `avg_waste_percentage`, `waste_event_count` |

**Dimension load order (strict — facts require dim SKs):**
```
DIM_DATE → DIM_CATEGORY → DIM_MEAL_PERIOD → DIM_WASTE_REASON
→ DIM_LOCATION → DIM_MENU → DIM_SUPPLIER
→ FACT_PRODUCTION → FACT_WASTE → FACT_CONSUMPTION → FACT_WASTE_SUMMARY
```

---

## Athena Analytics

**Database:** `food_waste_db`
**Workgroup:** `food-waste-wg` (1 GB per-query scan limit)
**Results bucket:** `{S3_BUCKET_NAME}-athena-results`

All 11 Gold tables are registered as external Parquet tables. Partitioned fact tables use `MSCK REPAIR TABLE` to load partitions.

**Five analytical queries** (`analytics/athena_queries.sql`):
1. Monthly waste trend by location — uses **LAG** window function
2. Top 10 menu items by waste cost — uses **DENSE_RANK**
3. Demand vs supply gap by category — uses **CTE**
4. Supplier quality score vs avg waste percentage — uses **window AVG**
5. Waste reason breakdown by meal period — uses **SUM OVER** window

**Root cause view** (`analytics/root_cause_view.sql`):
Classifies each location+category+month into one of four root causes:
- Overproduction (waste % > 40, high volume per event)
- Storage/Spoilage (perishable category, waste % > 20)
- Portion Mismatch (waste % > 30, low volume per event)
- Low Demand (everything else)

---

## Orchestration

**Tool:** Apache Airflow via Astro CLI (local Docker)
**DAG:** `food_waste_pipeline` (`orchestration/dags/food_waste_pipeline.py`)
**Trigger:** Manual only (`schedule=None`)

```
bronze_ingestion → silver_transform → gold_load
```

Each task is a `GlueJobOperator` with `wait_for_completion=True`. If any Glue job fails, the task raises and the DAG halts — downstream tasks do not run.

AWS credentials are stored in the Airflow `aws_default` connection (UI entry, never in code).

---

## Infrastructure Setup Scripts

Run in this order:
```bash
python aws_setup/01_s3_setup.py     # S3 buckets + folder structure + lifecycle rules
python aws_setup/02_iam_setup.py    # IAM role + managed + inline policies
python setup_aws.py                  # Glue scripts upload + Glue jobs + Athena
```

All scripts are idempotent — safe to re-run.

---

## CI/CD

**Tool:** GitHub Actions (`.github/workflows/pipeline_tests.yml`)
**Triggers:** Push and pull_request on all branches

Jobs:
1. Install dependencies, run `pytest tests/ -v`
2. Scan for hardcoded AWS credentials (AKIA pattern grep)

No AWS resources are touched in CI — tests use in-memory PySpark DataFrames.

---

## Cost Profile

| Component | Cost at rest | Cost per run |
|-----------|-------------|-------------|
| S3 (~1 GB data) | ~$0.02/month | — |
| Glue Bronze (Python shell) | $0 | ~$0.01 |
| Glue Silver (Spark 2×G.1X, ~5 min) | $0 | ~$0.14 |
| Glue Gold (Python shell) | $0 | ~$0.01 |
| Athena queries | $0 idle | ~$0.00 (< 1 GB scan) |
| Airflow (local Docker) | $0 | $0 |
| **Total** | **~$0.02/month** | **~$0.16/run** |
