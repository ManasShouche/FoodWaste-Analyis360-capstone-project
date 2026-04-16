"""
Glue Silver Job — Food Waste Optimization 360
Type: Glue Spark (glueetl) — uses GlueContext + PySpark

Glue job arguments:
  --S3_BUCKET    : data bucket (Bronze source + Silver destination)
  --RUN_DATE     : optional processing date (YYYY-MM-DD), defaults to today

Reads Bronze Parquet from s3://{S3_BUCKET}/bronze/
Writes Silver Parquet to   s3://{S3_BUCKET}/silver/year={Y}/month={M}/

Idempotent: dynamic partition overwrite — re-running same partition replaces it.
"""

import os
import sys
from datetime import date

# Glue Spark bootstrap
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ---------------------------------------------------------------------------
# Resolve Glue job arguments
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
job.init(args["JOB_NAME"], args)

os.environ["S3_BUCKET"] = args["S3_BUCKET"]

run_date = args.get("RUN_DATE", date.today().isoformat()) \
    if "RUN_DATE" in args else date.today().isoformat()

# Enable dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------------------------------------------------------------------
# Run Silver transform
# ---------------------------------------------------------------------------
from transforms.silver_transform import run_silver

print(f"=== Silver Job Start | date={run_date} ===")
run_silver(run_date)
print("=== Silver Job Complete ===")

job.commit()
