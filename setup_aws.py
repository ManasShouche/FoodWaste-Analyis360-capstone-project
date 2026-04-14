"""
setup_aws.py — Food Waste Optimization 360
==========================================
Idempotent AWS infrastructure provisioner.
Run once to create everything; safe to re-run — skips resources that already exist.

Resources created:
  1. S3 buckets         — data bucket + Athena results bucket
  2. IAM role           — AWSGlueServiceRole + S3/Athena inline policy
  3. Glue scripts       — package project code, upload to S3
  4. Glue jobs          — food_waste_bronze, food_waste_silver, food_waste_gold
  5. Athena workgroup   — 1 GB per-query scan limit
  6. Athena database    — food_waste_db
  7. Athena tables      — all Gold external tables from create_tables.sql

Usage:
  pip install boto3
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export AWS_REGION=ap-south-1
  export S3_BUCKET=my-food-waste-bucket
  export ATHENA_RESULTS_BUCKET=my-food-waste-athena
  python setup_aws.py
"""

import io
import json
import os
import re
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Load config from environment — NEVER hardcode
# ---------------------------------------------------------------------------
AWS_REGION            = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET             = os.environ.get("S3_BUCKET", "")
ATHENA_RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", "")
ATHENA_DATABASE       = os.environ.get("ATHENA_DATABASE", "food_waste_db")
ATHENA_WORKGROUP      = os.environ.get("ATHENA_WORKGROUP", "food-waste-wg")
GLUE_ROLE_NAME        = os.environ.get("GLUE_ROLE_NAME", "FoodWasteGlueRole")

PROJECT_ROOT = Path(__file__).parent

# Glue job → script file mapping
GLUE_JOBS = {
    "food_waste_bronze": "glue_scripts/glue_bronze.py",
    "food_waste_silver": "glue_scripts/glue_silver.py",
    "food_waste_gold":   "glue_scripts/glue_gold.py",
}

GLUE_JOB_TYPES = {
    "food_waste_bronze": "pythonshell",
    "food_waste_silver": "spark",
    "food_waste_gold":   "pythonshell",
}

GLUE_TIMEOUT_MINUTES = 30
SCAN_LIMIT_BYTES = 1_073_741_824   # 1 GB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ok(msg: str):
    print(f"  [OK]   {msg}")

def _skip(msg: str):
    print(f"  [SKIP] {msg}")

def _fail(msg: str):
    print(f"  [FAIL] {msg}")
    raise SystemExit(1)

def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _boto(service: str):
    return boto3.client(service, region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# 1. S3 Buckets
# ---------------------------------------------------------------------------
def setup_s3():
    _section("1 / 7 — S3 Buckets")
    s3 = _boto("s3")

    for bucket in [S3_BUCKET, ATHENA_RESULTS_BUCKET]:
        if not bucket:
            _fail(f"Bucket name is empty — check environment variables.")
        try:
            if AWS_REGION == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
                )
            # Block public access
            s3.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
            _ok(f"Created bucket: s3://{bucket}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                _skip(f"Bucket already exists: s3://{bucket}")
            else:
                _fail(f"S3 error for {bucket}: {e}")

    # Create folder stubs so Athena can see partitions later
    for prefix in ["bronze/", "silver/", "gold/dims/", "gold/facts/", "glue-scripts/"]:
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=prefix)
        except ClientError:
            pass
    _ok("S3 folder structure initialised.")


# ---------------------------------------------------------------------------
# 2. IAM Role for Glue
# ---------------------------------------------------------------------------
GLUE_TRUST_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
})

def _s3_inline_policy(data_bucket: str, athena_bucket: str) -> str:
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3DataAccess",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                           "s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": [
                    f"arn:aws:s3:::{data_bucket}",
                    f"arn:aws:s3:::{data_bucket}/*",
                    f"arn:aws:s3:::{athena_bucket}",
                    f"arn:aws:s3:::{athena_bucket}/*",
                ],
            },
            {
                "Sid": "AthenaAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "athena:GetWorkGroup",
                ],
                "Resource": "*",
            },
            {
                "Sid": "GlueCatalogAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:GetTable", "glue:GetTables",
                    "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
                    "glue:GetPartition", "glue:GetPartitions",
                    "glue:CreatePartition", "glue:BatchCreatePartition",
                ],
                "Resource": "*",
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup", "logs:CreateLogStream",
                    "logs:PutLogEvents", "logs:AssociateKmsKey",
                ],
                "Resource": "arn:aws:logs:*:*:/aws-glue/*",
            },
        ],
    })


def setup_iam():
    _section("2 / 7 — IAM Role")
    iam = boto3.client("iam")   # IAM is global — no region

    # Create role
    try:
        resp = iam.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=GLUE_TRUST_POLICY,
            Description="Glue service role for Food Waste Optimization 360",
        )
        role_arn = resp["Role"]["Arn"]
        _ok(f"Created IAM role: {GLUE_ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=GLUE_ROLE_NAME)["Role"]["Arn"]
            _skip(f"IAM role already exists: {GLUE_ROLE_NAME}")
        else:
            _fail(f"IAM role error: {e}")

    # Attach managed Glue policy
    try:
        iam.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        )
        _ok("Attached managed policy: AWSGlueServiceRole")
    except ClientError as e:
        if "already attached" in str(e).lower() or e.response["Error"]["Code"] == "LimitExceeded":
            _skip("AWSGlueServiceRole already attached")
        else:
            _fail(f"Policy attach error: {e}")

    # Put inline S3 + Athena policy
    iam.put_role_policy(
        RoleName=GLUE_ROLE_NAME,
        PolicyName="FoodWasteS3AthenaAccess",
        PolicyDocument=_s3_inline_policy(S3_BUCKET, ATHENA_RESULTS_BUCKET),
    )
    _ok("Upserted inline policy: FoodWasteS3AthenaAccess")

    # IAM propagation — Glue job creation fails if role isn't ready
    print("  Waiting 10s for IAM propagation...")
    time.sleep(10)

    return role_arn


# ---------------------------------------------------------------------------
# 3. Package project code + upload Glue scripts to S3
# ---------------------------------------------------------------------------
PACKAGES_TO_ZIP = ["ingestion", "transforms", "warehouse"]


def _build_zip() -> bytes:
    """Zip project packages into memory for --extra-py-files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for pkg in PACKAGES_TO_ZIP:
            pkg_path = PROJECT_ROOT / pkg
            if not pkg_path.exists():
                continue
            for py_file in pkg_path.rglob("*.py"):
                arcname = py_file.relative_to(PROJECT_ROOT)
                zf.write(py_file, arcname)
    buf.seek(0)
    return buf.read()


def setup_scripts():
    _section("3 / 7 — Upload Glue Scripts to S3")
    s3 = _boto("s3")

    # Upload project zip
    zip_bytes = _build_zip()
    zip_key = "glue-scripts/food_waste_360.zip"
    s3.put_object(Bucket=S3_BUCKET, Key=zip_key, Body=zip_bytes)
    _ok(f"Uploaded project zip: s3://{S3_BUCKET}/{zip_key}")

    # Upload each Glue job wrapper script
    for job_name, script_rel in GLUE_JOBS.items():
        script_path = PROJECT_ROOT / script_rel
        if not script_path.exists():
            _fail(f"Glue script not found: {script_path}  — run this after creating glue_scripts/")
        s3_key = f"glue-scripts/{script_path.name}"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=script_path.read_bytes(),
        )
        _ok(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")

    return f"s3://{S3_BUCKET}/glue-scripts/food_waste_360.zip"


# ---------------------------------------------------------------------------
# 4. Glue Jobs
# ---------------------------------------------------------------------------
def _glue_job_config(job_name: str, role_arn: str, extra_py_files_uri: str) -> dict:
    job_type = GLUE_JOB_TYPES[job_name]
    script_key = f"glue-scripts/{Path(GLUE_JOBS[job_name]).name}"

    base = {
        "Name": job_name,
        "Role": role_arn,
        "Command": {
            "Name": "glueetl" if job_type == "spark" else "pythonshell",
            "ScriptLocation": f"s3://{S3_BUCKET}/{script_key}",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--S3_BUCKET":         S3_BUCKET,
            "--ATHENA_DATABASE":   ATHENA_DATABASE,
            "--ATHENA_WORKGROUP":  ATHENA_WORKGROUP,
            "--extra-py-files":    extra_py_files_uri,
            "--job-language":      "python",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics":    "true",
        },
        # GlueVersion: Spark supports 4.0, Python shell max is 3.0
        "GlueVersion": "4.0" if job_type == "spark" else "3.0",
        "Timeout": GLUE_TIMEOUT_MINUTES,
        "Tags": {"project": "food-waste-360"},
    }

    if job_type == "spark":
        base["NumberOfWorkers"] = 2
        base["WorkerType"] = "G.1X"
    else:
        # Python shell: MaxCapacity in DPUs (0.0625 = minimum)
        base["MaxCapacity"] = 0.0625
        # Remove Spark-only fields
        base["Command"]["Name"] = "pythonshell"

    return base


def setup_glue_jobs(role_arn: str, extra_py_files_uri: str):
    _section("4 / 7 — Glue Jobs")
    glue = _boto("glue")

    for job_name in GLUE_JOBS:
        config = _glue_job_config(job_name, role_arn, extra_py_files_uri)
        try:
            glue.get_job(JobName=job_name)
            # Job exists — update it (Tags not allowed in JobUpdate)
            update_config = {k: v for k, v in config.items() if k not in ("Name", "Tags")}
            glue.update_job(JobName=job_name, JobUpdate=update_config)
            _skip(f"Updated existing Glue job: {job_name}  (timeout={GLUE_TIMEOUT_MINUTES}m)")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                glue.create_job(**config)
                _ok(f"Created Glue job: {job_name}  (type={GLUE_JOB_TYPES[job_name]}, timeout={GLUE_TIMEOUT_MINUTES}m)")
            else:
                _fail(f"Glue job error [{job_name}]: {e}")


# ---------------------------------------------------------------------------
# 5. Athena Workgroup
# ---------------------------------------------------------------------------
def setup_athena_workgroup():
    _section("5 / 7 — Athena Workgroup")
    athena = _boto("athena")

    wg_config = {
        "ResultConfiguration": {
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/query-results/",
        },
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": True,
        "BytesScannedCutoffPerQuery": SCAN_LIMIT_BYTES,
        "RequesterPaysEnabled": False,
        "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
    }

    try:
        athena.create_work_group(
            Name=ATHENA_WORKGROUP,
            Configuration=wg_config,
            Description="Food Waste Optimization 360 workgroup — 1 GB scan limit",
            Tags=[{"Key": "project", "Value": "food-waste-360"}],
        )
        _ok(f"Created Athena workgroup: {ATHENA_WORKGROUP}  (scan limit = 1 GB)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidRequestException" and \
                ("already exists" in str(e).lower() or "already created" in str(e).lower()):
            # Update scan limit in case it changed
            athena.update_work_group(
                WorkGroup=ATHENA_WORKGROUP,
                ConfigurationUpdates={
                    "BytesScannedCutoffPerQuery": SCAN_LIMIT_BYTES,
                    "ResultConfigurationUpdates": {
                        "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/query-results/",
                    },
                },
            )
            _skip(f"Athena workgroup already exists — settings updated: {ATHENA_WORKGROUP}")
        else:
            _fail(f"Athena workgroup error: {e}")


# ---------------------------------------------------------------------------
# 6. Athena Database
# ---------------------------------------------------------------------------
def _run_athena_ddl(query: str, athena) -> None:
    """Execute a DDL statement and poll until complete."""
    resp = athena.start_query_execution(
        QueryString=query,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/query-results/ddl/"
        },
    )
    qid = resp["QueryExecutionId"]
    for _ in range(60):           # poll up to 60s
        state = athena.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        detail = athena.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena DDL failed [{state}]: {detail}")


def setup_athena_database():
    _section("6 / 7 — Athena Database")
    athena = _boto("athena")
    _run_athena_ddl(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}", athena)
    _ok(f"Athena database ready: {ATHENA_DATABASE}")


# ---------------------------------------------------------------------------
# 7. Athena Table Registration
# ---------------------------------------------------------------------------
def setup_athena_tables():
    _section("7 / 7 — Athena External Tables")
    athena = _boto("athena")

    sql_path = PROJECT_ROOT / "analytics" / "create_tables.sql"
    if not sql_path.exists():
        _fail(f"create_tables.sql not found at {sql_path}")

    raw_sql = sql_path.read_text()
    # Replace the ${S3_BUCKET} placeholder
    raw_sql = raw_sql.replace("${S3_BUCKET}", S3_BUCKET)

    # Split into individual statements (split on ; followed by whitespace/newline)
    statements = [s.strip() for s in re.split(r";\s*\n", raw_sql) if s.strip()]

    table_count = 0
    for stmt in statements:
        if not stmt:
            continue
        # Skip the top-level CREATE DATABASE (already done)
        if stmt.upper().startswith("CREATE DATABASE"):
            continue
        try:
            _run_athena_ddl(stmt + ";", athena)
            # Extract table name for display
            match = re.search(
                r"(?:CREATE EXTERNAL TABLE|MSCK REPAIR TABLE)\s+(?:IF NOT EXISTS\s+)?"
                r"[\w\.]+\.([\w]+)",
                stmt, re.IGNORECASE,
            )
            label = match.group(1) if match else stmt[:50]
            _ok(f"{label}")
            table_count += 1
        except RuntimeError as e:
            if "alreadyexists" in str(e).lower() or "already exists" in str(e).lower():
                match = re.search(r"[\w\.]+\.([\w]+)", stmt, re.IGNORECASE)
                _skip(f"{match.group(1) if match else '?'} — already exists")
            else:
                _fail(f"Athena DDL failed: {e}\nStatement: {stmt[:200]}")

    _ok(f"Registered {table_count} table DDL statements.")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_env():
    required = {
        "S3_BUCKET":             S3_BUCKET,
        "ATHENA_RESULTS_BUCKET": ATHENA_RESULTS_BUCKET,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print("\n[ERROR] Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nCopy .env.example → .env, fill in values, then:\n  source .env && python setup_aws.py")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("  Food Waste Optimization 360 — AWS Setup")
    print(f"  Region:  {AWS_REGION}")
    print(f"  Bucket:  {S3_BUCKET}")
    print(f"  Athena:  {ATHENA_RESULTS_BUCKET}")
    print("=" * 60)

    validate_env()

    setup_s3()
    role_arn = setup_iam()
    extra_py_files_uri = setup_scripts()
    setup_glue_jobs(role_arn, extra_py_files_uri)
    setup_athena_workgroup()
    setup_athena_database()
    setup_athena_tables()

    print("\n" + "=" * 60)
    print("  SETUP COMPLETE")
    print(f"  Role ARN : {role_arn}")
    print(f"  Workgroup: {ATHENA_WORKGROUP}")
    print(f"  Database : {ATHENA_DATABASE}")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Add aws_default connection in Airflow UI (see DAG setup comment)")
    print("  2. Trigger pipeline: make run")
    print("  3. Launch dashboard: make dashboard")


if __name__ == "__main__":
    main()
