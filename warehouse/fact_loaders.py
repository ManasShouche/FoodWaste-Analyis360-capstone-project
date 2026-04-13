"""
Fact Loaders — Food Waste Optimization 360
Loads 4 fact tables from Silver data by resolving dimension surrogate keys.

Facts:
  FACT_PRODUCTION  — one row per production event
  FACT_WASTE       — one row per (date, location, menu, meal_period) waste event
  FACT_CONSUMPTION — derived; production vs waste vs consumed
  FACT_WASTE_SUMMARY — aggregated by location + category + year + month

All writes are partitioned by year, month (idempotent overwrite).
DQ gate is called before each write.
"""

import os
import uuid

import awswrangler as wr
import boto3
import pandas as pd


S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _read_dim(dim_name: str) -> pd.DataFrame:
    path = f"s3://{S3_BUCKET}/gold/dims/{dim_name}/data.parquet"
    return wr.s3.read_parquet(path=path)


def _write_fact(df: pd.DataFrame, fact_name: str) -> None:
    """Write fact table as a single Parquet file to Gold S3."""
    if "year" not in df.columns or "month" not in df.columns:
        raise ValueError(f"[{fact_name}] Missing year/month partition columns.")

    path = f"s3://{S3_BUCKET}/gold/facts/{fact_name}/data.parquet"
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=False)
    print(f"  Wrote {len(df)} rows → {path}")


def _pandas_dq(df: pd.DataFrame, checks: list[tuple], fact_name: str) -> None:
    """Pandas-based DQ gate (mirrors PySpark run_dq_gate signature)."""
    for check_name, mask in checks:
        failing = (~mask).sum()
        if failing > 0:
            raise ValueError(f"DQ GATE FAILED [{fact_name}] {check_name} — {failing} rows")
        print(f"  DQ PASSED [{fact_name}]: {check_name}")


def _read_silver() -> pd.DataFrame:
    path = f"s3://{S3_BUCKET}/silver/"
    df = wr.s3.read_parquet(path=path, dataset=True)
    # Partition keys come back as strings from directory names — cast to int
    df["year"]  = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    return df


# ---------------------------------------------------------------------------
# FACT_PRODUCTION
# ---------------------------------------------------------------------------
def load_fact_production(silver: pd.DataFrame, dim_date: pd.DataFrame,
                          dim_location: pd.DataFrame, dim_menu: pd.DataFrame,
                          dim_meal_period: pd.DataFrame) -> pd.DataFrame:
    print("Loading FACT_PRODUCTION ...")

    df = silver[["date", "location_id", "menu_item_id", "meal_period",
                 "quantity_prepared", "cost_per_unit", "batch_id", "year", "month"]].copy()
    df["full_date"] = df["date"].astype(str)

    # Resolve surrogate keys
    df = df.merge(dim_date[["full_date", "date_sk"]], on="full_date", how="left")
    df = df.merge(dim_location[["location_id", "location_sk"]], on="location_id", how="left")
    df = df.merge(dim_menu[["menu_item_id", "menu_sk"]], on="menu_item_id", how="left")
    dim_mp = dim_meal_period.rename(columns={"meal_period_name": "meal_period"})
    df = df.merge(dim_mp[["meal_period", "meal_period_sk"]], on="meal_period", how="left")

    df["production_sk"] = [str(uuid.uuid4()) for _ in range(len(df))]

    result = df[["production_sk", "date_sk", "location_sk", "menu_sk", "meal_period_sk",
                 "quantity_prepared", "cost_per_unit", "batch_id", "year", "month"]]

    _pandas_dq(result, [
        ("date_sk_not_null",       result["date_sk"].notna()),
        ("location_sk_not_null",   result["location_sk"].notna()),
        ("menu_sk_not_null",       result["menu_sk"].notna()),
        ("meal_period_sk_not_null", result["meal_period_sk"].notna()),
    ], "FACT_PRODUCTION")

    _write_fact(result, "fact_production")
    return result


# ---------------------------------------------------------------------------
# FACT_WASTE
# ---------------------------------------------------------------------------
def load_fact_waste(silver: pd.DataFrame, dim_date: pd.DataFrame,
                    dim_location: pd.DataFrame, dim_menu: pd.DataFrame,
                    dim_meal_period: pd.DataFrame, dim_waste_reason: pd.DataFrame,
                    dim_supplier: pd.DataFrame) -> pd.DataFrame:
    print("Loading FACT_WASTE ...")

    waste_cols = ["date", "location_id", "menu_item_id", "meal_period",
                  "quantity_wasted", "waste_percentage", "waste_cost",
                  "waste_reason", "year", "month"]
    df = silver[waste_cols].dropna(subset=["waste_reason"]).copy()
    df["full_date"] = df["date"].astype(str)

    df = df.merge(dim_date[["full_date", "date_sk"]], on="full_date", how="left")
    df = df.merge(dim_location[["location_id", "location_sk"]], on="location_id", how="left")

    # menu_sk + category_sk (via dim_menu)
    df = df.merge(
        dim_menu[["menu_item_id", "menu_sk", "category"]],
        on="menu_item_id", how="left",
    )
    # Resolve category_sk from dim_category via the category name on dim_menu
    # (dim_category is not passed here — carry category name for the join in fact_waste_summary;
    #  category_sk is resolved via a secondary merge on category column)
    # We need dim_category passed in — add it as a parameter for the category_sk lookup
    # For now store the category string directly so Query 2 can use dim_menu join path.

    dim_mp = dim_meal_period.rename(columns={"meal_period_name": "meal_period"})
    df = df.merge(dim_mp[["meal_period", "meal_period_sk"]], on="meal_period", how="left")

    dim_wr = dim_waste_reason.rename(columns={"waste_reason_name": "waste_reason"})
    df = df.merge(dim_wr[["waste_reason", "waste_reason_sk"]], on="waste_reason", how="left")

    # Supplier: join current record on menu_item_id
    current_sup = dim_supplier[dim_supplier["is_current"]].copy()
    df = df.merge(current_sup[["menu_item_id", "supplier_sk"]], on="menu_item_id", how="left")

    df["waste_sk"] = [str(uuid.uuid4()) for _ in range(len(df))]

    result = df[["waste_sk", "date_sk", "location_sk", "menu_sk", "meal_period_sk",
                 "waste_reason_sk", "supplier_sk",
                 "quantity_wasted", "waste_percentage", "waste_cost",
                 "year", "month"]]

    _pandas_dq(result, [
        ("date_sk_not_null",        result["date_sk"].notna()),
        ("location_sk_not_null",    result["location_sk"].notna()),
        ("menu_sk_not_null",        result["menu_sk"].notna()),
        ("meal_period_sk_not_null", result["meal_period_sk"].notna()),
        ("waste_cost_gte_0",        result["waste_cost"] >= 0),
    ], "FACT_WASTE")

    _write_fact(result, "fact_waste")
    return result


# ---------------------------------------------------------------------------
# FACT_CONSUMPTION
# ---------------------------------------------------------------------------
def load_fact_consumption(silver: pd.DataFrame, dim_date: pd.DataFrame,
                           dim_location: pd.DataFrame, dim_menu: pd.DataFrame) -> pd.DataFrame:
    print("Loading FACT_CONSUMPTION ...")

    cols = ["date", "location_id", "menu_item_id", "quantity_prepared",
            "quantity_wasted", "quantity_consumed", "demand_gap", "year", "month"]
    df = silver[cols].copy()
    df["full_date"] = df["date"].astype(str)

    df = df.merge(dim_date[["full_date", "date_sk"]], on="full_date", how="left")
    df = df.merge(dim_location[["location_id", "location_sk"]], on="location_id", how="left")
    df = df.merge(dim_menu[["menu_item_id", "menu_sk"]], on="menu_item_id", how="left")

    df["consumption_sk"] = [str(uuid.uuid4()) for _ in range(len(df))]

    result = df[["consumption_sk", "date_sk", "location_sk", "menu_sk",
                 "quantity_prepared", "quantity_wasted", "quantity_consumed", "demand_gap",
                 "year", "month"]]

    _pandas_dq(result, [
        ("consumed_gte_0", result["quantity_consumed"] >= 0),
        ("date_sk_not_null", result["date_sk"].notna()),
    ], "FACT_CONSUMPTION")

    _write_fact(result, "fact_consumption")
    return result


# ---------------------------------------------------------------------------
# FACT_WASTE_SUMMARY (aggregated)
# ---------------------------------------------------------------------------
def load_fact_waste_summary(silver: pd.DataFrame, dim_location: pd.DataFrame,
                             dim_category: pd.DataFrame) -> pd.DataFrame:
    print("Loading FACT_WASTE_SUMMARY ...")

    # Only include rows where waste actually occurred
    df = silver[silver["quantity_wasted"] > 0].copy()
    # Cast to plain string to avoid Categorical groupby size mismatch
    df["category_upper"] = df["category"].astype(str).str.upper().str.strip()

    agg = (
        df.groupby(["location_id", "category_upper", "year", "month"],
                   as_index=False, observed=True)
        .agg(
            total_waste_quantity=("quantity_wasted", "sum"),
            total_waste_cost=("waste_cost", "sum"),
            avg_waste_percentage=("waste_percentage", "mean"),
            waste_event_count=("quantity_wasted", "count"),
        )
    )

    agg = agg.merge(dim_location[["location_id", "location_sk"]], on="location_id", how="left")
    dim_cat = dim_category.rename(columns={"category_name": "category_upper"})
    agg = agg.merge(dim_cat[["category_upper", "category_sk"]], on="category_upper", how="left")

    result = agg[["location_sk", "category_sk", "category_upper", "year", "month",
                  "total_waste_quantity", "total_waste_cost", "avg_waste_percentage",
                  "waste_event_count"]]
    result = result.rename(columns={"category_upper": "category"})

    _pandas_dq(result, [
        ("totals_gt_0",          result["total_waste_quantity"] > 0),
        ("location_sk_not_null", result["location_sk"].notna()),
    ], "FACT_WASTE_SUMMARY")

    _write_fact(result, "fact_waste_summary")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_fact_loads() -> None:
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET not set.")

    silver = _read_silver()

    dim_date         = _read_dim("dim_date")
    dim_location     = _read_dim("dim_location")
    dim_menu         = _read_dim("dim_menu")
    dim_meal_period  = _read_dim("dim_meal_period")
    dim_waste_reason = _read_dim("dim_waste_reason")
    dim_supplier     = _read_dim("dim_supplier")
    dim_category     = _read_dim("dim_category")

    load_fact_production(silver, dim_date, dim_location, dim_menu, dim_meal_period)
    load_fact_waste(silver, dim_date, dim_location, dim_menu, dim_meal_period, dim_waste_reason, dim_supplier)
    load_fact_consumption(silver, dim_date, dim_location, dim_menu)
    load_fact_waste_summary(silver, dim_location, dim_category)

    print("\nAll fact tables loaded successfully.")


if __name__ == "__main__":
    run_fact_loads()
