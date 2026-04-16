
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

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
job.init(args["JOB_NAME"], args)

os.environ["S3_BUCKET"] = args["S3_BUCKET"]

run_date = args.get("RUN_DATE", date.today().isoformat()) \
    if "RUN_DATE" in args else date.today().isoformat()

# Enable dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


from transforms.silver_transform import run_silver

print(f"=== Silver Job Start | date={run_date} ===")
run_silver(run_date)
print("=== Silver Job Complete ===")

job.commit()
