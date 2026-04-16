
import os
import sys
import zipfile
import glob
from datetime import date


_extract_dir = "/tmp/fw360_pkg"
os.makedirs(_extract_dir, exist_ok=True)

_zip_candidates = [p for p in sys.path if isinstance(p, str) and "food_waste_360" in p and p.endswith(".zip")]
if not _zip_candidates:
    _zip_candidates = glob.glob("/tmp/**/*food_waste_360*.zip", recursive=True) + \
                      glob.glob("/tmp/*food_waste_360*.zip")

if _zip_candidates:
    with zipfile.ZipFile(_zip_candidates[0], "r") as _z:
        _z.extractall(_extract_dir)
    print(f"Extracted project zip to {_extract_dir}")
else:
    print("WARN: project zip not found — assuming packages already on sys.path")

if _extract_dir not in sys.path:
    sys.path.insert(0, _extract_dir)

import awswrangler as wr
import boto3
import pandas as pd

# Glue Python shell arg parsing
try:
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["S3_BUCKET"])
    os.environ["S3_BUCKET"] = _args["S3_BUCKET"]
    try:
        _db = getResolvedOptions(sys.argv, ["ATHENA_DATABASE", "ATHENA_WORKGROUP"])
        os.environ["ATHENA_DATABASE"]  = _db["ATHENA_DATABASE"]
        os.environ["ATHENA_WORKGROUP"] = _db["ATHENA_WORKGROUP"]
    except (Exception, SystemExit):
        pass
    try:
        _dt = getResolvedOptions(sys.argv, ["RUN_DATE"])
        run_date = date.fromisoformat(_dt["RUN_DATE"])
    except (Exception, SystemExit):
        run_date = date.today()
except ImportError:
    run_date = date.today()

from warehouse.dim_loaders import run_dim_loads
from warehouse.scd2_supplier import run_scd2_supplier
from warehouse.fact_loaders import run_fact_loads

S3_BUCKET  = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")


# ---------------------------------------------------------------------------
# Data readers
# ---------------------------------------------------------------------------
def _s3():
    return boto3.client("s3", region_name=AWS_REGION)


def read_silver() -> pd.DataFrame:
    """Read all Silver partitions from S3."""
    df = wr.s3.read_parquet(path=f"s3://{S3_BUCKET}/silver/", dataset=True)
    df["year"]  = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    return df


def read_supplier_from_bronze() -> pd.DataFrame:
    """
    Read supplier_data from Bronze S3.

    Supplier data is NOT in Silver — the Silver transform only joins
    production + waste + menu + location. Supplier must be sourced
    directly from the Bronze layer for SCD2 processing.

    Reads all date partitions and returns the latest state per supplier_id
    (highest effective_from date where is_current is true or blank).
    """
    path = f"s3://{S3_BUCKET}/bronze/source=supplier_data/"

    try:
        df = wr.s3.read_parquet(path=path, dataset=True)
    except Exception as e:
        print(f"  [WARN] Could not read supplier Bronze data: {e}")
        return pd.DataFrame()

    if df.empty:
        print("  [WARN] No supplier Bronze data found.")
        return pd.DataFrame()

    # Strip whitespace from string cols
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Cast types
    df["lead_time_days"] = pd.to_numeric(df["lead_time_days"], errors="coerce").astype("Int64")
    df["quality_score"]  = pd.to_numeric(df["quality_score"],  errors="coerce")

    # Keep only current records (is_current == '1' or True or empty effective_to)
    if "is_current" in df.columns:
        current = df[
            df["is_current"].astype(str).str.strip().isin(["1", "True", "true", "1.0"])
        ].copy()
        if current.empty:
            # Fallback: use all rows, deduplicate to latest by effective_from
            current = df.copy()
    else:
        current = df.copy()

    # One row per supplier_id — latest effective_from
    if "effective_from" in current.columns:
        current["effective_from"] = pd.to_datetime(
            current["effective_from"], errors="coerce"
        )
        current = (
            current.sort_values("effective_from", ascending=False)
            .drop_duplicates("supplier_id")
            .reset_index(drop=True)
        )
    else:
        current = current.drop_duplicates("supplier_id").reset_index(drop=True)

    # Select only the columns SCD2 needs
    keep = ["supplier_id", "supplier_name", "menu_item_id",
            "supplier_city", "lead_time_days", "quality_score"]
    available = [c for c in keep if c in current.columns]
    result = current[available].dropna(subset=["supplier_id"]).reset_index(drop=True)

    print(f"  Supplier records loaded from Bronze: {len(result)}")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
print("=== Gold Job Start ===")
print(f"  run_date = {run_date}")

# ── Read Silver (for dims + facts that derive from it) ──────────────────────
silver_df = read_silver()
print(f"  Silver rows: {len(silver_df):,}")

# ── Step 1: Static + SCD1 dimensions ──────────────────────────────────────
print("\n[Step 1] Loading static + SCD1 dimensions...")
run_dim_loads(silver_df)

# ── Step 2: DIM_SUPPLIER (SCD2) — source: Bronze, not Silver ──────────────
print("\n[Step 2] Loading DIM_SUPPLIER (SCD2) from Bronze...")
supplier_df = read_supplier_from_bronze()
if supplier_df.empty:
    print("  [WARN] No supplier data — DIM_SUPPLIER will be empty.")
else:
    run_scd2_supplier(supplier_df, run_date=run_date)

# ── Step 3: Fact tables ────────────────────────────────────────────────────
print("\n[Step 3] Loading fact tables...")
run_fact_loads()

print("\n=== Gold Job Complete ===")
