
import os
from pyathena import connect


def get_connection():

    aws_region          = os.environ.get("AWS_REGION", "ap-south-1")
    athena_results_bucket = os.environ.get("ATHENA_RESULTS_BUCKET")
    athena_database     = os.environ.get("ATHENA_DATABASE", "food_waste_db")

    if not athena_results_bucket:
        raise EnvironmentError("ATHENA_RESULTS_BUCKET environment variable is not set.")

    return connect(
        s3_staging_dir=f"s3://{athena_results_bucket}/query-results/",
        region_name=aws_region,
        schema_name=athena_database,
    )
