"""
Unit tests for transforms/silver_transform.py

Tests derived column calculations and DQ gate enforcement.
Uses in-memory PySpark DataFrames — no AWS calls.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
from datetime import date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transforms.silver_transform import build_silver, silver_dq_checks
from transforms.quality_checks import run_dq_gate


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_transforms")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


PROD_SCHEMA = StructType([
    StructField("date",                  DateType(),   True),
    StructField("location_id",           StringType(), True),
    StructField("menu_item_id",          StringType(), True),
    StructField("meal_period",           StringType(), True),
    StructField("quantity_prepared",     DoubleType(), True),
    StructField("cost_per_unit",         DoubleType(), True),
    StructField("planned_quantity",      DoubleType(), True),
    StructField("actual_served_estimate",DoubleType(), True),
    StructField("batch_id",              StringType(), True),
])

WASTE_SCHEMA = StructType([
    StructField("date",           DateType(),   True),
    StructField("location_id",    StringType(), True),
    StructField("menu_item_id",   StringType(), True),
    StructField("meal_period",    StringType(), True),
    StructField("quantity_wasted",DoubleType(), True),
    StructField("waste_reason",   StringType(), True),
    StructField("waste_stage",    StringType(), True),
])

MENU_SCHEMA = StructType([
    StructField("menu_item_id",   StringType(), True),
    StructField("menu_item_name", StringType(), True),
    StructField("category",       StringType(), True),
    StructField("sub_category",   StringType(), True),
    StructField("veg_flag",       StringType(), True),
    StructField("cost_per_unit",  DoubleType(), True),
    StructField("shelf_life_hours",IntegerType(),True),
    StructField("prep_complexity",StringType(), True),
])

LOC_SCHEMA = StructType([
    StructField("location_id",    StringType(), True),
    StructField("location_name",  StringType(), True),
    StructField("city",           StringType(), True),
    StructField("region",         StringType(), True),
    StructField("location_type",  StringType(), True),
    StructField("capacity",       StringType(), True),
    StructField("storage_rating", StringType(), True),
])


def make_silver(spark, prod_rows, waste_rows=None):
    prod = spark.createDataFrame(prod_rows or [], schema=PROD_SCHEMA)
    waste = spark.createDataFrame(waste_rows or [], schema=WASTE_SCHEMA)
    menu = spark.createDataFrame(
        [("MI001", "Dal Rice", "MAIN COURSE", "sub", "Yes", 67.0, 24, "Low")],
        schema=MENU_SCHEMA,
    )
    loc = spark.createDataFrame(
        [("LOC001", "Campus Cafe", "Bangalore", "South", "Campus", "500", "A")],
        schema=LOC_SCHEMA,
    )
    return build_silver(prod, waste, menu, loc)


# ---------------------------------------------------------------------------
# Derived column: waste_percentage
# ---------------------------------------------------------------------------
def test_waste_percentage_calculation(spark):
    prod_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 100.0, 50.0, 110.0, 90.0, "b1")]
    waste_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 20.0, "overproduction", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    row = silver.select("waste_percentage").first()
    assert abs(row["waste_percentage"] - 20.0) < 0.01


# ---------------------------------------------------------------------------
# Derived column: waste_cost
# ---------------------------------------------------------------------------
def test_waste_cost_calculation(spark):
    prod_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 100.0, 50.0, 110.0, 90.0, "b1")]
    waste_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 20.0, "spoilage", "Storage")]
    silver = make_silver(spark, prod_rows, waste_rows)
    row = silver.select("waste_cost").first()
    assert abs(row["waste_cost"] - 1000.0) < 0.01  # 20 * 50


# ---------------------------------------------------------------------------
# Derived column: quantity_consumed
# ---------------------------------------------------------------------------
def test_quantity_consumed_calculation(spark):
    prod_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 100.0, 50.0, 110.0, 90.0, "b1")]
    waste_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 30.0, "low_demand", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    row = silver.select("quantity_consumed").first()
    assert abs(row["quantity_consumed"] - 70.0) < 0.01


# ---------------------------------------------------------------------------
# DQ gate: waste > prepared must FAIL
# ---------------------------------------------------------------------------
def test_dq_fails_when_waste_exceeds_prepared(spark):
    prod_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 50.0, 50.0, 60.0, 40.0, "b1")]
    waste_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 80.0, "overproduction", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    with pytest.raises(ValueError, match="DQ GATE FAILED"):
        run_dq_gate(silver, [
            ("waste_lte_prepared", F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ])


# ---------------------------------------------------------------------------
# DQ gate: valid row passes all silver checks
# ---------------------------------------------------------------------------
def test_dq_passes_for_valid_row(spark):
    prod_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 100.0, 50.0, 110.0, 90.0, "b1")]
    waste_rows = [(date(2025, 1, 1), "LOC001", "MI001", "lunch", 10.0, "overproduction", "Serving")]
    silver = make_silver(spark, prod_rows, waste_rows)
    # Should not raise
    run_dq_gate(silver, [
        ("waste_lte_prepared",     F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ("waste_pct_0_to_100",     F.col("waste_percentage").between(0, 100)),
        ("location_id_not_null",   F.col("location_id").isNotNull()),
        ("quantity_consumed_gte_0",F.col("quantity_consumed") >= 0),
    ])
