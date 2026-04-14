"""
Glue Bronze Job — Food Waste Optimization 360
Type: Python shell (no Spark needed — uses boto3/pandas)

Glue job arguments (passed via --job-name and DefaultArguments):
  --S3_BUCKET        : target S3 bucket for Bronze output
  --DATA_DATE        : optional override date (YYYY-MM-DD), defaults to today

Reads macro_*.csv from s3://{S3_BUCKET}/raw/  OR from the DATA_DIR env var.
Uploads Parquet files to s3://{S3_BUCKET}/bronze/source={name}/date={date}/data.parquet
"""

import os
import sys
import zipfile
import glob

# ---------------------------------------------------------------------------
# Unzip project package so submodules are importable in Glue Python shell.
# Glue adds extra-py-files zips to sys.path as zip files; Python's zipimport
# sometimes fails to resolve sub-packages. Extracting to /tmp is more reliable.
# ---------------------------------------------------------------------------
_extract_dir = "/tmp/fw360_pkg"
os.makedirs(_extract_dir, exist_ok=True)

# Search sys.path entries and /tmp for the project zip
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

# Glue Python shell arg parsing
try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ["S3_BUCKET"])
    os.environ["S3_BUCKET"] = args["S3_BUCKET"]
    # Optional date override — catch SystemExit raised by argparse when arg is absent
    try:
        extra = getResolvedOptions(sys.argv, ["DATA_DATE"])
        os.environ["DATA_DATE"] = extra["DATA_DATE"]
    except (Exception, SystemExit):
        pass
except ImportError:
    # Running locally — env vars already set
    pass

# Ensure project packages are importable (uploaded via --extra-py-files)
from ingestion.bronze_loader import load_bronze

if __name__ == "__main__":
    print("=== Bronze Job Start ===")
    # Default: read CSVs from s3://bucket/raw/ (uploaded before the Glue job runs)
    data_dir = os.environ.get("DATA_DIR", f"s3://{os.environ.get('S3_BUCKET', '')}/raw/")
    load_bronze(data_dir=data_dir)
    print("=== Bronze Job Complete ===")
