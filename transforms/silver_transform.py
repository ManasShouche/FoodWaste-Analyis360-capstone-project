"""
Silver Transform — Food Waste Optimization 360
Reads all 5 sources from Bronze S3, cleans, joins, derives columns,
runs 10 DQ gates, and writes partitioned Parquet to Silver S3.

Idempotent: re-running for the same year/month overwrites that partition.
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType

# Allow local imports when running as a Glue job
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transforms.quality_checks import run_dq_gate

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FoodWaste_SilverTransform")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bronze read helpers
# ---------------------------------------------------------------------------
def read_bronze(spark: SparkSession, source: str, date: str) -> DataFrame:
    path = f"s3://{S3_BUCKET}/bronze/source={source}/date={date}/data.parquet"
    return spark.read.parquet(path)


# ---------------------------------------------------------------------------
# Cleaning functions (one per source)
# ---------------------------------------------------------------------------
def clean_production(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd").cast(DateType()))
        .withColumn("quantity_prepared", F.col("quantity_prepared").cast(DoubleType()))
        .withColumn("cost_per_unit", F.col("cost_per_unit").cast(DoubleType()))
        .withColumn("planned_quantity", F.col("planned_quantity").cast(DoubleType()))
        .withColumn("actual_served_estimate", F.col("actual_served_estimate").cast(DoubleType()))
        .withColumn("location_id", F.trim(F.col("location_id")))
        .withColumn("menu_item_id", F.trim(F.col("menu_item_id")))
        .withColumn("meal_period", F.trim(F.lower(F.col("meal_period"))))
        .withColumn("category", F.trim(F.col("category")))
    )


def clean_waste(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd").cast(DateType()))
        .withColumn("quantity_wasted", F.col("quantity_wasted").cast(DoubleType()))
        .withColumn("location_id", F.trim(F.col("location_id")))
        .withColumn("menu_item_id", F.trim(F.col("menu_item_id")))
        .withColumn("meal_period", F.trim(F.lower(F.col("meal_period"))))
        # Normalise waste_reason: strip + lowercase
        .withColumn("waste_reason", F.trim(F.lower(F.col("waste_reason"))))
        .withColumn("waste_stage", F.trim(F.col("waste_stage")))
    )


def clean_menu(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("menu_item_id", F.trim(F.col("menu_item_id")))
        .withColumn("menu_item_name", F.trim(F.col("menu_item_name")))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("sub_category", F.trim(F.col("sub_category")))
        .withColumn("cost_per_unit", F.col("cost_per_unit").cast(DoubleType()))
        .withColumn("shelf_life_hours", F.col("shelf_life_hours").cast(IntegerType()))
    )


def clean_location(df: DataFrame) -> DataFrame:
    # Deduplicate on location_id — keep first occurrence
    return (
        df.withColumn("location_id", F.trim(F.col("location_id")))
        .withColumn("location_name", F.trim(F.col("location_name")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("region", F.trim(F.col("region")))
        .dropDuplicates(["location_id"])
    )


def clean_supplier(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("supplier_id", F.trim(F.col("supplier_id")))
        .withColumn("supplier_name", F.trim(F.col("supplier_name")))
        .withColumn("menu_item_id", F.trim(F.col("menu_item_id")))
        .withColumn("lead_time_days", F.col("lead_time_days").cast(IntegerType()))
        .withColumn("quality_score", F.col("quality_score").cast(DoubleType()))
        .withColumn(
            "effective_from",
            F.to_date(F.col("effective_from"), "yyyy-MM-dd").cast(DateType()),
        )
    )


# ---------------------------------------------------------------------------
# Silver transform
# ---------------------------------------------------------------------------
def build_silver(
    prod_df: DataFrame,
    waste_df: DataFrame,
    menu_df: DataFrame,
    loc_df: DataFrame,
) -> DataFrame:
    """
    Join production + waste + menu + location and compute derived columns.
    Aggregates waste to production grain (date+location+menu+meal_period)
    before joining to handle many waste rows per production row.
    """
    # Aggregate waste to match production grain
    waste_agg = waste_df.groupBy(
        "date", "location_id", "menu_item_id", "meal_period"
    ).agg(
        F.sum("quantity_wasted").alias("quantity_wasted"),
        F.first("waste_reason").alias("waste_reason"),
        F.first("waste_stage").alias("waste_stage"),
    )

    # Menu: keep distinct menu_item_id rows (drop dupes)
    # Exclude cost_per_unit, menu_item_name, category — production already carries them
    # as denormalised columns; keeping both causes duplicate column errors on write
    menu_dedup = menu_df.dropDuplicates(["menu_item_id"]).select(
        "menu_item_id", "sub_category", "veg_flag", "shelf_life_hours", "prep_complexity",
    )

    # Location: already deduped
    # Exclude location_name — production already carries it as a denormalised column
    loc_dedup = loc_df.select(
        "location_id", "city", "region", "location_type", "capacity", "storage_rating",
    )

    # Joins
    silver = (
        prod_df.join(waste_agg, on=["date", "location_id", "menu_item_id", "meal_period"], how="left")
        .join(menu_dedup, on="menu_item_id", how="left")
        .join(loc_dedup, on="location_id", how="left")
    )

    # Coalesce waste to 0 where no waste record exists
    silver = silver.withColumn(
        "quantity_wasted", F.coalesce(F.col("quantity_wasted"), F.lit(0.0))
    )

    # Derived columns
    silver = (
        silver
        .withColumn(
            "waste_percentage",
            F.when(
                F.col("quantity_prepared") > 0,
                (F.col("quantity_wasted") / F.col("quantity_prepared")) * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "waste_cost",
            F.col("quantity_wasted") * F.col("cost_per_unit"),
        )
        .withColumn(
            "quantity_consumed",
            F.col("quantity_prepared") - F.col("quantity_wasted"),
        )
        .withColumn(
            "demand_gap",
            F.col("planned_quantity") - F.col("actual_served_estimate"),
        )
        # Partition columns
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
    )

    return silver


# ---------------------------------------------------------------------------
# DQ gate definitions for Silver
# ---------------------------------------------------------------------------
VALID_WASTE_REASONS = ["overproduction", "spoilage", "low demand", "plate waste",
                       "forecast miss", "prep error", None]


def silver_dq_checks(df: DataFrame) -> list:
    valid_reasons_col = F.col("waste_reason").isin(
        [r for r in VALID_WASTE_REASONS if r is not None]
    ) | F.col("waste_reason").isNull()

    return [
        ("waste_lte_prepared",     F.col("quantity_wasted") <= F.col("quantity_prepared")),
        ("waste_pct_0_to_100",     F.col("waste_percentage").between(0, 100)),
        ("location_id_not_null",   F.col("location_id").isNotNull()),
        ("menu_item_id_not_null",  F.col("menu_item_id").isNotNull()),
        ("date_not_null",          F.col("date").isNotNull()),
        ("valid_waste_reason",     valid_reasons_col),
        ("waste_cost_gte_0",       F.col("waste_cost") >= 0),
        ("quantity_consumed_gte_0", F.col("quantity_consumed") >= 0),
        ("quantity_prepared_gt_0", F.col("quantity_prepared") > 0),
        ("waste_pct_not_null",     F.col("waste_percentage").isNotNull()),
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_silver(date: str) -> None:
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET environment variable is not set.")

    spark = get_spark()
    print(f"Silver transform started | date={date}")

    # Read Bronze
    prod_raw  = read_bronze(spark, "production_logs", date)
    waste_raw = read_bronze(spark, "waste_logs",      date)
    menu_raw  = read_bronze(spark, "menu_data",       date)
    loc_raw   = read_bronze(spark, "location_data",   date)

    # Clean
    prod  = clean_production(prod_raw)
    waste = clean_waste(waste_raw)
    menu  = clean_menu(menu_raw)
    loc   = clean_location(loc_raw)

    # Build Silver
    silver = build_silver(prod, waste, menu, loc)

    # DQ gate — hard stop before write
    run_dq_gate(silver, silver_dq_checks(silver))

    # Write — partition overwrite (idempotent)
    out_path = f"s3://{S3_BUCKET}/silver/"
    (
        silver.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out_path)
    )

    count = silver.count()
    print(f"Silver write complete: {count} rows → {out_path}")
    spark.stop()


if __name__ == "__main__":
    import sys
    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    run_silver(run_date)
