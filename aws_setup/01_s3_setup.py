"""
aws_setup/01_s3_setup.py — Food Waste Optimization 360
=======================================================
Creates and configures all S3 buckets and folder structure required by the pipeline.

Buckets created:
  {S3_BUCKET_NAME}                  — main data bucket (bronze / silver / gold)
  {S3_BUCKET_NAME}-athena-results   — Athena query output

Folder prefixes created (empty placeholder objects) inside the main bucket:
  bronze/          silver/         gold/dims/
  gold/facts/      glue-scripts/   glue-temp/

Per-bucket configuration applied:
  ✓ Public access blocked on both buckets
  ✓ Versioning enabled on main bucket
  ✓ Lifecycle rule: expire bronze/ objects after 90 days

Idempotent: safe to re-run — existing resources are detected and skipped.

Usage:
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export AWS_REGION=ap-south-1        # defaults to ap-south-1 if unset
  export S3_BUCKET_NAME=my-food-waste

  python aws_setup/01_s3_setup.py

No IAM resources are created here — see 02_iam_setup.py.
"""

import os
import sys

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Config — loaded from environment only
# ---------------------------------------------------------------------------
AWS_REGION      = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME  = os.environ.get("S3_BUCKET_NAME", "").strip()

MAIN_BUCKET     = S3_BUCKET_NAME
ATHENA_BUCKET   = f"{S3_BUCKET_NAME}-athena-results"

BRONZE_LIFECYCLE_DAYS = 90

FOLDER_PREFIXES = [
    "bronze/",
    "silver/",
    "gold/dims/",
    "gold/facts/",
    "glue-scripts/",
    "glue-temp/",
]

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


def _s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# Step 1 — Create bucket
# ---------------------------------------------------------------------------
def _bucket_exists(s3_client, bucket: str) -> bool:
    """Return True if the bucket exists and is owned by this account."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            return False
        if code == "403":
            # Bucket exists but belongs to another account
            _fail(
                f"Bucket '{bucket}' exists but access was denied (403). "
                "Choose a different bucket name."
            )
        raise


def create_bucket(s3_client, bucket: str) -> None:
    if _bucket_exists(s3_client, bucket):
        _skip(f"Bucket already exists:  s3://{bucket}")
        return

    try:
        if AWS_REGION == "us-east-1":
            # us-east-1 does not accept LocationConstraint
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        _ok(f"Created bucket:         s3://{bucket}")
    except ClientError as e:
        _fail(f"create_bucket [{bucket}]: {e}")


# ---------------------------------------------------------------------------
# Step 2 — Block all public access
# ---------------------------------------------------------------------------
def block_public_access(s3_client, bucket: str) -> None:
    try:
        existing = s3_client.get_public_access_block(Bucket=bucket)
        cfg = existing["PublicAccessBlockConfiguration"]
        if all([
            cfg.get("BlockPublicAcls"),
            cfg.get("IgnorePublicAcls"),
            cfg.get("BlockPublicPolicy"),
            cfg.get("RestrictPublicBuckets"),
        ]):
            _skip(f"Public access already blocked: s3://{bucket}")
            return
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchPublicAccessBlockConfiguration":
            _fail(f"get_public_access_block [{bucket}]: {e}")

    try:
        s3_client.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            },
        )
        _ok(f"Blocked public access:  s3://{bucket}")
    except ClientError as e:
        _fail(f"put_public_access_block [{bucket}]: {e}")


# ---------------------------------------------------------------------------
# Step 3 — Enable versioning (main bucket only)
# ---------------------------------------------------------------------------
def enable_versioning(s3_client, bucket: str) -> None:
    try:
        resp = s3_client.get_bucket_versioning(Bucket=bucket)
        if resp.get("Status") == "Enabled":
            _skip(f"Versioning already enabled: s3://{bucket}")
            return
    except ClientError as e:
        _fail(f"get_bucket_versioning [{bucket}]: {e}")

    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        _ok(f"Versioning enabled:     s3://{bucket}")
    except ClientError as e:
        _fail(f"put_bucket_versioning [{bucket}]: {e}")


# ---------------------------------------------------------------------------
# Step 4 — Lifecycle rule: expire bronze/ after 90 days
# ---------------------------------------------------------------------------
LIFECYCLE_RULE_ID = "expire-bronze-90d"


def _lifecycle_rule_exists(s3_client, bucket: str) -> bool:
    try:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        return any(r["ID"] == LIFECYCLE_RULE_ID for r in resp.get("Rules", []))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            return False
        _fail(f"get_bucket_lifecycle_configuration [{bucket}]: {e}")


def apply_lifecycle_rule(s3_client, bucket: str) -> None:
    if _lifecycle_rule_exists(s3_client, bucket):
        _skip(f"Lifecycle rule already set: '{LIFECYCLE_RULE_ID}' on s3://{bucket}")
        return

    # Fetch any existing rules so we don't wipe them
    try:
        existing = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = existing.get("Rules", [])
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            rules = []
        else:
            _fail(f"get_bucket_lifecycle_configuration [{bucket}]: {e}")

    rules.append({
        "ID":     LIFECYCLE_RULE_ID,
        "Status": "Enabled",
        "Filter": {"Prefix": "bronze/"},
        "Expiration": {"Days": BRONZE_LIFECYCLE_DAYS},
        # Also expire non-current versions so versioning doesn't accumulate bronze data
        "NoncurrentVersionExpiration": {"NoncurrentDays": BRONZE_LIFECYCLE_DAYS},
    })

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": rules},
        )
        _ok(
            f"Lifecycle rule applied:  bronze/ objects expire after "
            f"{BRONZE_LIFECYCLE_DAYS} days  (s3://{bucket})"
        )
    except ClientError as e:
        _fail(f"put_bucket_lifecycle_configuration [{bucket}]: {e}")


# ---------------------------------------------------------------------------
# Step 5 — Create folder prefixes (empty placeholder objects)
# ---------------------------------------------------------------------------
def create_folder_prefixes(s3_client, bucket: str, prefixes: list[str]) -> None:
    for prefix in prefixes:
        try:
            # Check if the placeholder already exists
            s3_client.head_object(Bucket=bucket, Key=prefix)
            _skip(f"Folder prefix exists:   s3://{bucket}/{prefix}")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                s3_client.put_object(Bucket=bucket, Key=prefix, Body=b"")
                _ok(f"Created folder prefix:  s3://{bucket}/{prefix}")
            else:
                _fail(f"head_object [{bucket}/{prefix}]: {e}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_env() -> None:
    if not S3_BUCKET_NAME:
        print(
            "\n[ERROR] S3_BUCKET_NAME is not set.\n"
            "  export S3_BUCKET_NAME=your-unique-bucket-name\n"
            "  python aws_setup/01_s3_setup.py",
            file=sys.stderr,
        )
        sys.exit(1)

    # S3 bucket names: 3–63 chars, lowercase alphanumeric and hyphens only
    import re
    if not re.fullmatch(r"[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]", S3_BUCKET_NAME):
        _fail(
            f"Invalid bucket name '{S3_BUCKET_NAME}'. "
            "Must be 3–63 lowercase alphanumeric chars or hyphens, "
            "and cannot start or end with a hyphen."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    validate_env()

    s3 = _s3()

    print(f"\n{'═' * 56}")
    print(f"  S3 Setup — Food Waste Optimization 360")
    print(f"  Region : {AWS_REGION}")
    print(f"  Main   : s3://{MAIN_BUCKET}")
    print(f"  Athena : s3://{ATHENA_BUCKET}")
    print(f"{'═' * 56}")

    # ── Main data bucket ─────────────────────────────────────
    _header("Main data bucket")
    create_bucket(s3, MAIN_BUCKET)
    block_public_access(s3, MAIN_BUCKET)
    enable_versioning(s3, MAIN_BUCKET)
    apply_lifecycle_rule(s3, MAIN_BUCKET)

    _header("Folder prefixes  →  s3://" + MAIN_BUCKET)
    create_folder_prefixes(s3, MAIN_BUCKET, FOLDER_PREFIXES)

    # ── Athena results bucket ─────────────────────────────────
    _header("Athena results bucket")
    create_bucket(s3, ATHENA_BUCKET)
    block_public_access(s3, ATHENA_BUCKET)
    # No versioning on results bucket — Athena regenerates outputs on every query

    # ── Summary ──────────────────────────────────────────────
    print(f"\n{'═' * 56}")
    print("  S3 setup complete.")
    print(f"  Main bucket   : s3://{MAIN_BUCKET}")
    print(f"  Athena bucket : s3://{ATHENA_BUCKET}")
    print(f"{'═' * 56}\n")


if __name__ == "__main__":
    main()
