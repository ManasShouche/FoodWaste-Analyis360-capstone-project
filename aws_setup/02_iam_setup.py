"""
aws_setup/02_iam_setup.py — Food Waste Optimization 360
========================================================
Creates the IAM role used by all three Glue jobs and attaches the
permissions they need to access S3, Athena, and the Glue Data Catalog.

Resources created:
  IAM Role       : {GLUE_ROLE_NAME}  (default: FoodWasteGlueRole)
  Trust policy   : allows glue.amazonaws.com to assume the role
  Managed policy : arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
  Inline policy  : FoodWasteS3AthenaAccess
                   — S3 read/write on main + athena-results buckets
                   — Athena query execution
                   — Glue Data Catalog (database + table operations)
                   — CloudWatch Logs writes (Glue job output)

Idempotent: safe to re-run — existing role/attachments are detected and skipped.

Prerequisites:
  - Run 01_s3_setup.py first (bucket names must exist for the S3 policy ARNs)

Usage:
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export S3_BUCKET_NAME=my-food-waste
  export GLUE_ROLE_NAME=FoodWasteGlueRole     # optional, this is the default
  python aws_setup/02_iam_setup.py

Note: IAM is a global service — AWS_REGION is not used here.
"""

import json
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
S3_BUCKET_NAME  = os.environ.get("S3_BUCKET_NAME", "").strip()
ATHENA_BUCKET   = f"{S3_BUCKET_NAME}-athena-results"
GLUE_ROLE_NAME  = os.environ.get("GLUE_ROLE_NAME", "FoodWasteGlueRole").strip()

MANAGED_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
INLINE_POLICY_NAME = "FoodWasteS3AthenaAccess"

GLUE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
def _ok(msg: str)   -> None: print(f"  [OK]      {msg}")
def _skip(msg: str) -> None: print(f"  [SKIP]    {msg}")
def _info(msg: str) -> None: print(f"  [INFO]    {msg}")
def _fail(msg: str) -> None:
    print(f"  [FAIL]    {msg}", file=sys.stderr)
    sys.exit(1)

def _header(title: str) -> None:
    print(f"\n{'─' * 56}")
    print(f"  {title}")
    print(f"{'─' * 56}")


def _iam() -> boto3.client:
    return boto3.client("iam")   # IAM is global — no region param


# ---------------------------------------------------------------------------
# Inline policy document
# ---------------------------------------------------------------------------
def _inline_policy(data_bucket: str, athena_bucket: str) -> str:
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3DataBucketAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
                "Resource": [
                    f"arn:aws:s3:::{data_bucket}",
                    f"arn:aws:s3:::{data_bucket}/*",
                    f"arn:aws:s3:::{athena_bucket}",
                    f"arn:aws:s3:::{athena_bucket}/*",
                ],
            },
            {
                "Sid": "AthenaQueryAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "athena:GetWorkGroup",
                    "athena:ListWorkGroups",
                ],
                "Resource": "*",
            },
            {
                "Sid": "GlueCatalogAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:UpdatePartition",
                ],
                "Resource": "*",
            },
            {
                "Sid": "CloudWatchLogsAccess",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:AssociateKmsKey",
                ],
                "Resource": "arn:aws:logs:*:*:/aws-glue/*",
            },
        ],
    }, indent=2)


# ---------------------------------------------------------------------------
# Step 1 — Create IAM role
# ---------------------------------------------------------------------------
def create_role(iam) -> str:
    """Create the Glue service role. Returns the role ARN."""
    try:
        resp = iam.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(GLUE_TRUST_POLICY),
            Description="Glue execution role for Food Waste Optimization 360",
            Tags=[{"Key": "project", "Value": "food-waste-360"}],
        )
        role_arn = resp["Role"]["Arn"]
        _ok(f"Created IAM role:  {GLUE_ROLE_NAME}")
        _info(f"ARN: {role_arn}")
        return role_arn
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=GLUE_ROLE_NAME)["Role"]["Arn"]
            _skip(f"Role already exists: {GLUE_ROLE_NAME}")
            _info(f"ARN: {role_arn}")
            return role_arn
        _fail(f"create_role: {e}")


# ---------------------------------------------------------------------------
# Step 2 — Attach managed policy
# ---------------------------------------------------------------------------
def attach_managed_policy(iam) -> None:
    # Check if already attached
    try:
        attached = iam.list_attached_role_policies(RoleName=GLUE_ROLE_NAME)[
            "AttachedPolicies"
        ]
        already = [p for p in attached if p["PolicyArn"] == MANAGED_POLICY_ARN]
        if already:
            _skip(f"Managed policy already attached: AWSGlueServiceRole")
            return
    except ClientError as e:
        _fail(f"list_attached_role_policies: {e}")

    try:
        iam.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn=MANAGED_POLICY_ARN,
        )
        _ok("Attached managed policy: AWSGlueServiceRole")
    except ClientError as e:
        _fail(f"attach_role_policy: {e}")


# ---------------------------------------------------------------------------
# Step 3 — Put inline policy (idempotent — put always overwrites)
# ---------------------------------------------------------------------------
def put_inline_policy(iam) -> None:
    policy_doc = _inline_policy(S3_BUCKET_NAME, ATHENA_BUCKET)
    try:
        iam.put_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyName=INLINE_POLICY_NAME,
            PolicyDocument=policy_doc,
        )
        _ok(f"Upserted inline policy: {INLINE_POLICY_NAME}")
        _info("Covers: S3 (both buckets), Athena, Glue Catalog, CloudWatch Logs")
    except ClientError as e:
        _fail(f"put_role_policy: {e}")


# ---------------------------------------------------------------------------
# Verification — describe what was set up
# ---------------------------------------------------------------------------
def verify_role(iam, role_arn: str) -> None:
    _header("Verification")
    try:
        role = iam.get_role(RoleName=GLUE_ROLE_NAME)["Role"]
        _info(f"Role name       : {role['RoleName']}")
        _info(f"Role ARN        : {role['Arn']}")
        _info(f"Created         : {role['CreateDate'].strftime('%Y-%m-%d %H:%M UTC')}")

        managed = iam.list_attached_role_policies(RoleName=GLUE_ROLE_NAME)[
            "AttachedPolicies"
        ]
        for p in managed:
            _info(f"Managed policy  : {p['PolicyName']}")

        inline = iam.list_role_policies(RoleName=GLUE_ROLE_NAME)["PolicyNames"]
        for p in inline:
            _info(f"Inline policy   : {p}")
    except ClientError as e:
        _info(f"Could not verify role: {e}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_env() -> None:
    if not S3_BUCKET_NAME:
        print(
            "\n[ERROR] S3_BUCKET_NAME is not set.\n"
            "  export S3_BUCKET_NAME=your-bucket-name\n"
            "  python aws_setup/02_iam_setup.py",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    validate_env()

    iam = _iam()

    print(f"\n{'═' * 56}")
    print(f"  IAM Setup — Food Waste Optimization 360")
    print(f"  Role      : {GLUE_ROLE_NAME}")
    print(f"  S3 bucket : {S3_BUCKET_NAME}")
    print(f"{'═' * 56}")

    _header("Step 1 — IAM Role")
    role_arn = create_role(iam)

    _header("Step 2 — Managed Policy")
    attach_managed_policy(iam)

    _header("Step 3 — Inline S3 / Athena / Glue Catalog Policy")
    put_inline_policy(iam)

    # IAM changes take a few seconds to propagate globally.
    # Glue job creation will fail with AccessDeniedException if done immediately.
    _header("Propagation wait")
    print("  Waiting 10 s for IAM global propagation before proceeding...")
    time.sleep(10)
    _ok("Wait complete.")

    verify_role(iam, role_arn)

    print(f"\n{'═' * 56}")
    print("  IAM setup complete.")
    print(f"  Role ARN : {role_arn}")
    print(f"\n  Next step: python aws_setup/03_glue_setup.py")
    print(f"{'═' * 56}\n")


if __name__ == "__main__":
    main()
