"""
Unit tests for transforms/quality_checks.py

Uses in-memory PySpark DataFrames — no AWS calls.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transforms.quality_checks import run_dq_gate


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_quality_checks")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("id",               StringType(), True),
    StructField("quantity_wasted",  DoubleType(), True),
    StructField("quantity_prepared",DoubleType(), True),
    StructField("waste_pct",        DoubleType(), True),
])


# ---------------------------------------------------------------------------
# run_dq_gate raises on failing check
# ---------------------------------------------------------------------------
def test_null_check_fails(spark):
    """Null in a required column should raise ValueError."""
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", None, 80.0, None)],
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("quantity_wasted_not_null", F.col("quantity_wasted").isNotNull()),
        ])


def test_range_check_fails(spark):
    """waste_pct > 100 should fail a 0–100 range check."""
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 90.0, 80.0, 112.5)],
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("waste_pct_0_to_100", F.col("waste_pct").between(0, 100)),
        ])


def test_duplicate_check_fails(spark):
    """Duplicate ids should fail a uniqueness check."""
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("A", 20.0, 100.0, 20.0)],
        schema=SCHEMA,
    )
    deduped_count = df.groupBy("id").count().filter(F.col("count") > 1).count()
    # Manually verify there are dupes, then run the check
    assert deduped_count > 0
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("no_duplicate_ids",
             F.col("id").isNotNull() &
             (df.groupBy("id").count().filter(F.col("count") == 1).count() > 0
              or F.lit(False))
             ),
        ])


def test_waste_lte_prepared_fails(spark):
    """waste > prepared must fail."""
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 110.0, 100.0, 110.0)],
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(df, [
            ("waste_lte_prepared", F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ])


# ---------------------------------------------------------------------------
# run_dq_gate passes silently when all checks pass
# ---------------------------------------------------------------------------
def test_all_checks_pass(spark):
    """All valid rows should not raise."""
    df = spark.createDataFrame(
        [("A", 10.0, 100.0, 10.0), ("B", 20.0, 80.0, 25.0)],
        schema=SCHEMA,
    )
    # Should complete without exception
    run_dq_gate(df, [
        ("not_null",         F.col("quantity_wasted").isNotNull()),
        ("pct_range",        F.col("waste_pct").between(0, 100)),
        ("waste_lte_prep",   F.col("quantity_wasted") <= F.col("quantity_prepared")),
    ])


def test_empty_checks_list_passes(spark):
    """Empty check list should never raise."""
    df = spark.createDataFrame([("X", 5.0, 10.0, 50.0)], schema=SCHEMA)
    run_dq_gate(df, [])
