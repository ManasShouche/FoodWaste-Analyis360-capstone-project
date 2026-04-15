# SETUP:
# 1. cd orchestration && astro dev init
# 2. astro dev start
# 3. Go to http://localhost:8080  (login: admin / admin)
# 4. Admin > Connections > New Connection
#    Conn Id:   aws_default
#    Conn Type: Amazon Web Services
#    Extra:     {"aws_access_key_id": "...", "aws_secret_access_key": "...", "region_name": "ap-south-1"}
# 5. astro dev run dags trigger food_waste_pipeline

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "food-waste-360",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------
def on_failure_callback(context):
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    logger.error(
        "Task FAILED | task_id=%s | execution_date=%s | dag_id=%s",
        task_id,
        execution_date,
        context.get("dag").dag_id,
    )
    print(f"[FAILURE] task={task_id}  at={datetime.utcnow().isoformat()}Z")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="food_waste_pipeline",
    description="Food Waste Optimization 360 — Bronze → Silver → Gold pipeline",
    schedule=None,           # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    tags=["food-waste", "batch", "medallion"],
) as dag:

    bronze_ingestion = GlueJobOperator(
        task_id="bronze_ingestion",
        job_name="food_waste_bronze",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
        verbose=False,
        on_failure_callback=on_failure_callback,
    )

    silver_transform = GlueJobOperator(
        task_id="silver_transform",
        job_name="food_waste_silver",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
        verbose=False,
        on_failure_callback=on_failure_callback,
    )

    gold_load = GlueJobOperator(
        task_id="gold_load",
        job_name="food_waste_gold",
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=True,
        verbose=False,
        on_failure_callback=on_failure_callback,
    )

    # Task dependency chain
    bronze_ingestion >> silver_transform >> gold_load
