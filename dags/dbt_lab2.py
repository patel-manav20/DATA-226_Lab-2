from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

DBT_DIR = "/opt/airflow/dbt"

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dbt_lab2_pipeline",
    start_date=days_ago(1),
    schedule= None,  # 02:15 daily, after the Lab 1 ETL DAG
    catchup=False,
    default_args=default_args,
    tags=["lab2","dbt"],
) as dag:

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_etl",
        external_dag_id="stock_ingest_yf",  # <-- set to your Lab 1 ETL DAG id
        external_task_id=None,                    # wait for the whole ETL DAG to be success
        poke_interval=60,
        timeout=60*60,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
        env=os.environ,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
        env=os.environ,
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_DIR}",
        env=os.environ,
    )

    wait_for_etl >> dbt_run >> dbt_test >> dbt_snapshot
