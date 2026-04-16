"""
Dimension Loaders (SCD1 + static dims) — Food Waste Optimization 360
Loads 6 of the 7 dimension tables to Gold S3 using pandas.
DIM_SUPPLIER (SCD2) is handled separately in scd2_supplier.py.

Gold S3 path: s3://{S3_BUCKET}/gold/dims/{dim_name}/data.parquet
"""

import os
import uuid
from datetime import date

import awswrangler as wr
import boto3
import pandas as pd

S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def write_dim(df: pd.DataFrame, dim_name: str) -> None:
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET not set.")
    path = f"s3://{S3_BUCKET}/gold/dims/{dim_name}/data.parquet"
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=False)
    print(f"  Wrote {len(df)} rows → {path}")


def read_silver_parquet() -> pd.DataFrame:
    """Read all Silver partitions from S3."""
    path = f"s3://{S3_BUCKET}/silver/"
    df = wr.s3.read_parquet(path=path, dataset=True)
    df["year"]  = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    return df


# ---------------------------------------------------------------------------
# DQ helper
# ---------------------------------------------------------------------------
def check_no_duplicate_sk(df: pd.DataFrame, sk_col: str, dim_name: str) -> None:
    dupes = df[sk_col].duplicated().sum()
    if dupes > 0:
        raise ValueError(f"DQ GATE FAILED [{dim_name}]: {dupes} duplicate surrogate keys in '{sk_col}'.")
    print(f"  DQ PASSED [{dim_name}]: no duplicate surrogate keys.")


# ---------------------------------------------------------------------------
# DIM_DATE — generated programmatically for full year 2025
# ---------------------------------------------------------------------------
def load_dim_date() -> pd.DataFrame:
    print("Loading DIM_DATE ...")
    dates = pd.date_range("2025-01-01", "2026-12-31", freq="D")
    df = pd.DataFrame({
        "date_sk":    dates.strftime("%Y%m%d").astype(int),
        "full_date":  dates.strftime("%Y-%m-%d"),
        "day_of_week": dates.day_of_week + 1,          # 1=Mon … 7=Sun
        "day_name":   dates.day_name(),
        "month":      dates.month,
        "month_name": dates.month_name(),
        "quarter":    dates.quarter,
        "year":       dates.year,
        "is_weekend": dates.day_of_week >= 5,
    })
    check_no_duplicate_sk(df, "date_sk", "DIM_DATE")
    write_dim(df, "dim_date")
    return df


# ---------------------------------------------------------------------------
# DIM_CATEGORY — distinct categories from Silver
# ---------------------------------------------------------------------------
def load_dim_category(silver_df: pd.DataFrame) -> pd.DataFrame:
    print("Loading DIM_CATEGORY ...")
    categories = silver_df["category"].dropna().str.upper().str.strip().unique()
    df = pd.DataFrame({
        "category_sk":   [str(uuid.uuid4()) for _ in categories],
        "category_name": categories,
    })
    check_no_duplicate_sk(df, "category_sk", "DIM_CATEGORY")
    write_dim(df, "dim_category")
    return df


# ---------------------------------------------------------------------------
# DIM_MEAL_PERIOD — static
# ---------------------------------------------------------------------------
def load_dim_meal_period() -> pd.DataFrame:
    print("Loading DIM_MEAL_PERIOD ...")
    periods = ["breakfast", "lunch", "dinner", "snack"]
    df = pd.DataFrame({
        "meal_period_sk":   [str(uuid.uuid4()) for _ in periods],
        "meal_period_name": periods,
    })
    check_no_duplicate_sk(df, "meal_period_sk", "DIM_MEAL_PERIOD")
    write_dim(df, "dim_meal_period")
    return df


# ---------------------------------------------------------------------------
# DIM_WASTE_REASON — static (all normalised reason values)
# ---------------------------------------------------------------------------
def load_dim_waste_reason() -> pd.DataFrame:
    print("Loading DIM_WASTE_REASON ...")
    reasons = ["overproduction", "spoilage", "low demand", "plate waste",
               "forecast miss", "prep error"]
    df = pd.DataFrame({
        "waste_reason_sk":   [str(uuid.uuid4()) for _ in reasons],
        "waste_reason_name": reasons,
    })
    check_no_duplicate_sk(df, "waste_reason_sk", "DIM_WASTE_REASON")
    write_dim(df, "dim_waste_reason")
    return df


# ---------------------------------------------------------------------------
# DIM_LOCATION — SCD1 upsert on location_id
# ---------------------------------------------------------------------------
def load_dim_location(silver_df: pd.DataFrame) -> pd.DataFrame:
    print("Loading DIM_LOCATION ...")
    src = (
        silver_df[["location_id", "location_name", "city", "region",
                   "location_type", "capacity", "storage_rating"]]
        .drop_duplicates("location_id")
        .reset_index(drop=True)
    )

    # Try to read existing dim
    try:
        existing = wr.s3.read_parquet(
            path=f"s3://{S3_BUCKET}/gold/dims/dim_location/data.parquet"
        )
    except Exception:
        existing = pd.DataFrame(columns=["location_sk", "location_id"])

    # SCD1: upsert — update existing, insert new
    merged = src.merge(existing[["location_id", "location_sk"]], on="location_id", how="left")
    merged["location_sk"] = merged["location_sk"].apply(
        lambda sk: sk if pd.notna(sk) else str(uuid.uuid4())
    )

    df = merged[["location_sk", "location_id", "location_name", "city", "region",
                 "location_type", "capacity", "storage_rating"]]
    check_no_duplicate_sk(df, "location_sk", "DIM_LOCATION")
    write_dim(df, "dim_location")
    return df


# ---------------------------------------------------------------------------
# DIM_MENU — SCD1 upsert on menu_item_id
# ---------------------------------------------------------------------------
def load_dim_menu(silver_df: pd.DataFrame) -> pd.DataFrame:
    print("Loading DIM_MENU ...")
    src = (
        silver_df[["menu_item_id", "menu_item_name", "category", "sub_category",
                   "veg_flag", "shelf_life_hours", "prep_complexity"]]
        .drop_duplicates("menu_item_id")
        .reset_index(drop=True)
    )

    try:
        existing = wr.s3.read_parquet(
            path=f"s3://{S3_BUCKET}/gold/dims/dim_menu/data.parquet"
        )
    except Exception:
        existing = pd.DataFrame(columns=["menu_item_id", "menu_sk"])

    merged = src.merge(existing[["menu_item_id", "menu_sk"]], on="menu_item_id", how="left")
    merged["menu_sk"] = merged["menu_sk"].apply(
        lambda sk: sk if pd.notna(sk) else str(uuid.uuid4())
    )

    df = merged[["menu_sk", "menu_item_id", "menu_item_name", "category", "sub_category",
                 "veg_flag", "shelf_life_hours", "prep_complexity"]]
    check_no_duplicate_sk(df, "menu_sk", "DIM_MENU")
    write_dim(df, "dim_menu")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_dim_loads(silver_df: pd.DataFrame) -> None:
    load_dim_date()
    load_dim_category(silver_df)
    load_dim_meal_period()
    load_dim_waste_reason()
    load_dim_location(silver_df)
    load_dim_menu(silver_df)
    print("\nAll SCD1 + static dimensions loaded successfully.")


if __name__ == "__main__":
    silver_df = read_silver_parquet()
    run_dim_loads(silver_df)
