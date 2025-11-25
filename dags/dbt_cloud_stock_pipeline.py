from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Airflow connection ID for dbt Cloud (the one you just created)
DBT_CLOUD_CONN_ID = "dbt_conn"

# Replace this with your actual dbt Cloud job id (an integer)
DBT_CLOUD_JOB_ID = 70471823532928

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_cloud_stock_pipeline",
    description="Trigger dbt Cloud job for Lab 2 stock transformations",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  
    catchup=False,
    max_active_runs=1,
    tags=["lab2", "dbt-cloud", "stocks"],
) as dag:

    run_dbt_cloud_job = DbtCloudRunJobOperator(
        task_id="run_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_CLOUD_JOB_ID,
        wait_for_termination=True,  # wait until dbt Cloud run finishes
        check_interval=60,          # check status every 60 seconds
        timeout=3600,               # give up after 1 hour
    )
