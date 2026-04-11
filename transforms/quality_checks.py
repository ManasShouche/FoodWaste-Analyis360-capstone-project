"""
Quality Checks — Food Waste Optimization 360
Shared DQ gate utility used by Silver and Gold layers.

Usage:
    from transforms.quality_checks import run_dq_gate
    run_dq_gate(df, [
        ("no_null_location", col("location_id").isNotNull()),
        ("waste_lte_prepared", col("quantity_wasted") <= col("quantity_prepared")),
    ])
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

logger = logging.getLogger(__name__)


def run_dq_gate(df: DataFrame, checks: list[tuple[str, Column]]) -> None:
    """
    Hard-stop DQ gate for PySpark DataFrames.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.
    checks : list of (check_name, spark_column_condition) tuples
        Each condition should evaluate to True for VALID rows.
        Rows where the condition is False are counted as failures.

    Raises
    ------
    ValueError
        If any check finds one or more failing rows.
    """
    for check_name, condition in checks:
        failing = df.filter(~condition).count()
        if failing > 0:
            raise ValueError(f"DQ GATE FAILED: {check_name} — {failing} rows")
        logger.info("DQ PASSED: %s", check_name)
        print(f"  DQ PASSED: {check_name}")
