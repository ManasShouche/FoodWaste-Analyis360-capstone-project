"""
SCD2 Loader for DIM_SUPPLIER — Food Waste Optimization 360

Logic:
  - New supplier_id  → insert as current record
  - Existing with tracked column change → close old, insert new
  - Existing unchanged → no action

Tracked columns: supplier_name, lead_time_days, quality_score

DQ gates enforced after every load:
  1. Only one is_current=True per supplier_id
  2. No overlapping effective/expiry date ranges per supplier_id
  3. All supplier_sk values are unique
"""

import os
import io
import uuid
from datetime import date, timedelta
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd

S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

TRACKED_COLS = ["supplier_name", "lead_time_days", "quality_score"]
FAR_FUTURE = date(9999, 12, 31)
DIM_KEY = "gold/dims/dim_supplier/data.parquet"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def _s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)


def _read_existing() -> Optional[pd.DataFrame]:
    """Return existing DIM_SUPPLIER DataFrame, or None if it doesn't exist yet."""
    try:
        resp = _s3().get_object(Bucket=S3_BUCKET, Key=DIM_KEY)
        buf = io.BytesIO(resp["Body"].read())
        return pd.read_parquet(buf)
    except _s3().exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def _write_dim(df: pd.DataFrame) -> None:
    path = f"s3://{S3_BUCKET}/{DIM_KEY}"
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=False)
    print(f"  Wrote {len(df)} rows → {path}")


# ---------------------------------------------------------------------------
# DQ checks
# ---------------------------------------------------------------------------
def run_scd2_dq(df: pd.DataFrame) -> None:
    # 1. Only one is_current=True per supplier_id
    current_counts = df[df["is_current"]].groupby("supplier_id").size()
    multi = current_counts[current_counts > 1]
    if not multi.empty:
        raise ValueError(
            f"DQ GATE FAILED [DIM_SUPPLIER]: multiple is_current=True rows for: {multi.index.tolist()}"
        )
    print("  DQ PASSED: one is_current=True per supplier_id")

    # 2. All supplier_sk values unique
    dupes = df["supplier_sk"].duplicated().sum()
    if dupes > 0:
        raise ValueError(f"DQ GATE FAILED [DIM_SUPPLIER]: {dupes} duplicate supplier_sk values.")
    print("  DQ PASSED: all supplier_sk unique")

    # 3. No overlapping date ranges per supplier_id
    for sid, grp in df.groupby("supplier_id"):
        grp_sorted = grp.sort_values("effective_date").reset_index(drop=True)
        for i in range(1, len(grp_sorted)):
            prev_expiry = grp_sorted.loc[i - 1, "expiry_date"]
            curr_effective = grp_sorted.loc[i, "effective_date"]
            if isinstance(prev_expiry, str):
                prev_expiry = date.fromisoformat(prev_expiry)
            if isinstance(curr_effective, str):
                curr_effective = date.fromisoformat(curr_effective)
            if prev_expiry >= curr_effective:
                raise ValueError(
                    f"DQ GATE FAILED [DIM_SUPPLIER]: overlapping date ranges for supplier_id={sid}"
                )
    print("  DQ PASSED: no overlapping date ranges")


# ---------------------------------------------------------------------------
# SCD2 merge logic
# ---------------------------------------------------------------------------
def apply_scd2(existing: Optional[pd.DataFrame], incoming: pd.DataFrame, today: date) -> pd.DataFrame:
    """
    Merge incoming supplier data with existing DIM_SUPPLIER using SCD2 logic.

    Parameters
    ----------
    existing  : current DIM_SUPPLIER (None on first load)
    incoming  : new supplier records from Silver (one row per supplier_id,
                representing the latest state)
    today     : reference date for effective/expiry dating
    """
    yesterday = today - timedelta(days=1)

    # Normalise incoming
    incoming = incoming.copy()
    incoming["lead_time_days"] = incoming["lead_time_days"].astype("Int64")
    incoming["quality_score"] = incoming["quality_score"].astype(float)
    incoming["supplier_name"] = incoming["supplier_name"].str.strip()

    if existing is None or existing.empty:
        # First load — insert all as new current records
        new_rows = incoming.copy()
        new_rows["supplier_sk"] = [str(uuid.uuid4()) for _ in range(len(new_rows))]
        new_rows["effective_date"] = today.isoformat()
        new_rows["expiry_date"] = FAR_FUTURE.isoformat()
        new_rows["is_current"] = True
        return new_rows[_dim_cols()]

    result = existing.copy()

    for _, new_row in incoming.iterrows():
        sid = new_row["supplier_id"]
        current_rec = result[(result["supplier_id"] == sid) & (result["is_current"])]

        if current_rec.empty:
            # New supplier — insert
            insert = _build_row(new_row, today, FAR_FUTURE, True)
            result = pd.concat([result, pd.DataFrame([insert])], ignore_index=True)
        else:
            idx = current_rec.index[0]
            changed = any(
                str(result.at[idx, c]) != str(new_row.get(c, ""))
                for c in TRACKED_COLS
            )
            if changed:
                # Close existing record
                result.at[idx, "expiry_date"] = yesterday.isoformat()
                result.at[idx, "is_current"] = False
                # Insert new current record
                insert = _build_row(new_row, today, FAR_FUTURE, True)
                result = pd.concat([result, pd.DataFrame([insert])], ignore_index=True)
            # else: no change — do nothing

    return result[_dim_cols()]


def _build_row(src: pd.Series, eff: date, exp: date, current: bool) -> dict:
    return {
        "supplier_sk":     str(uuid.uuid4()),
        "supplier_id":     src["supplier_id"],
        "supplier_name":   src["supplier_name"],
        "menu_item_id":    src.get("menu_item_id", None),
        "supplier_city":   src.get("supplier_city", None),
        "lead_time_days":  src.get("lead_time_days", None),
        "quality_score":   src.get("quality_score", None),
        "effective_date":  eff.isoformat(),
        "expiry_date":     exp.isoformat(),
        "is_current":      current,
    }


def _dim_cols() -> list[str]:
    return [
        "supplier_sk", "supplier_id", "supplier_name", "menu_item_id",
        "supplier_city", "lead_time_days", "quality_score",
        "effective_date", "expiry_date", "is_current",
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_scd2_supplier(incoming_df: pd.DataFrame, run_date: Optional[date] = None) -> pd.DataFrame:
    """
    Entry point.  Pass the Silver supplier DataFrame (latest state, one row per
    supplier_id) and the run date.  Returns the updated DIM_SUPPLIER DataFrame.
    """
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET not set.")

    today = run_date or date.today()
    print(f"SCD2 Supplier load | run_date={today}")

    existing = _read_existing()
    updated = apply_scd2(existing, incoming_df, today)
    run_scd2_dq(updated)
    _write_dim(updated)

    print(f"DIM_SUPPLIER updated: {len(updated)} total rows.")
    return updated


if __name__ == "__main__":
    # Supplier data must be read from Bronze, not Silver.
    # Silver only joins production + waste + menu + location.
    import awswrangler as wr
    import pandas as pd

    path = f"s3://{S3_BUCKET}/bronze/source=supplier_data/"
    supplier_raw = wr.s3.read_parquet(path=path, dataset=True)

    # Strip and cast
    for col in supplier_raw.select_dtypes(include="object").columns:
        supplier_raw[col] = supplier_raw[col].str.strip()
    supplier_raw["lead_time_days"] = pd.to_numeric(
        supplier_raw["lead_time_days"], errors="coerce"
    ).astype("Int64")
    supplier_raw["quality_score"] = pd.to_numeric(
        supplier_raw["quality_score"], errors="coerce"
    )

    # Keep current records only; one row per supplier_id (latest effective_from)
    if "is_current" in supplier_raw.columns:
        current = supplier_raw[
            supplier_raw["is_current"].astype(str).str.strip().isin(
                ["1", "True", "true", "1.0"]
            )
        ].copy()
    else:
        current = supplier_raw.copy()

    if "effective_from" in current.columns:
        current["effective_from"] = pd.to_datetime(current["effective_from"], errors="coerce")
        current = current.sort_values("effective_from", ascending=False)

    keep = ["supplier_id", "supplier_name", "menu_item_id",
            "supplier_city", "lead_time_days", "quality_score"]
    available = [c for c in keep if c in current.columns]
    supplier_latest = (
        current[available]
        .drop_duplicates("supplier_id")
        .dropna(subset=["supplier_id"])
        .reset_index(drop=True)
    )

    run_scd2_supplier(supplier_latest)
