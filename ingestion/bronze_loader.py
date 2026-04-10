"""
Bronze Loader — Food Waste Optimization 360
Reads macro_*.csv files from either:
  - S3 path  (s3://{bucket}/raw/)  — Glue execution
  - Local dir (data/raw/)          — local dev / testing

Adds metadata columns, converts to Parquet, runs DQ gates, uploads to Bronze S3.

S3 path: s3://{S3_BUCKET}/bronze/source={source_name}/date={today}/data.parquet
"""

import os
import io
import uuid
from datetime import datetime, timezone

import awswrangler as wr
import boto3
import pandas as pd


# ---------------------------------------------------------------------------
# Config (all from environment — never hardcoded)
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
DATA_DIR = os.environ.get("DATA_DIR", "data/raw")

# Map source names to filenames and their primary key column
SOURCES = {
    "production_logs": {
        "file": "macro_production_logs.csv",
        "pk": ["date", "location_id", "menu_item_id", "meal_period"],
    },
    "waste_logs": {
        "file": "macro_waste_logs.csv",
        "pk": ["date", "location_id", "menu_item_id", "meal_period"],
    },
    "menu_data": {
        "file": "macro_menu_data.csv",
        "pk": ["menu_item_id"],
    },
    "location_data": {
        "file": "macro_location_data.csv",
        "pk": ["location_id"],
    },
    "supplier_data": {
        "file": "macro_supplier_data.csv",
        "pk": ["supplier_record_id"],
    },
}


# ---------------------------------------------------------------------------
# DQ gate
# ---------------------------------------------------------------------------
def run_bronze_dq(df: pd.DataFrame, source_name: str, pk_cols: list[str]) -> None:
    """Hard stop: row count > 0, PK columns not null."""
    if len(df) == 0:
        raise ValueError(f"DQ GATE FAILED [{source_name}]: DataFrame is empty (0 rows).")

    for col in pk_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(
                f"DQ GATE FAILED [{source_name}]: PK column '{col}' has {null_count} null values."
            )

    print(f"  DQ PASSED [{source_name}]: {len(df)} rows, PK columns clean.")


# ---------------------------------------------------------------------------
# CSV readers (local or S3)
# ---------------------------------------------------------------------------
def _read_csv(data_dir: str, filename: str) -> pd.DataFrame:
    """Read a CSV from local path or S3 prefix."""
    if data_dir.startswith("s3://"):
        # data_dir is like s3://bucket/raw/
        s3_path = data_dir.rstrip("/") + "/" + filename
        bucket, key = s3_path[5:].split("/", 1)
        resp = boto3.client("s3", region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(resp["Body"].read()), dtype=str)
    else:
        return pd.read_csv(os.path.join(data_dir, filename), dtype=str)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def load_bronze(data_dir: str = DATA_DIR) -> None:
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET environment variable is not set.")

    batch_id = str(uuid.uuid4())
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    print(f"Bronze load started | batch_id={batch_id} | date={today}")
    print(f"Reading CSVs from: {data_dir}\n")

    for source_name, meta in SOURCES.items():
        print(f"Processing: {source_name} ({meta['file']})")

        try:
            df = _read_csv(data_dir, meta["file"])

            # Strip whitespace from all string columns
            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

            # DQ gate
            run_bronze_dq(df, source_name, meta["pk"])

            # Add metadata columns
            df["ingestion_timestamp"] = ingestion_ts
            df["source_file"] = meta["file"]
            df["batch_id"] = batch_id

            # Upload to S3 as Parquet
            s3_key = f"bronze/source={source_name}/date={today}/data.parquet"
            s3_path = f"s3://{S3_BUCKET}/{s3_key}"
            wr.s3.to_parquet(df=df, path=s3_path, index=False, dataset=False)

            print(f"  UPLOADED: {s3_path}  ({len(df)} rows)\n")

        except ValueError as dq_err:
            print(f"  FAILED: {dq_err}\n")
            raise
        except Exception as err:
            print(f"  ERROR [{source_name}]: {err}\n")
            raise

    print("Bronze load complete.")


if __name__ == "__main__":
    load_bronze()
