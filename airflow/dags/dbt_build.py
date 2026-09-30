from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Тот же URI, что и в DAG загрузки
RAW_DATASET = Dataset("clickhouse://clickhouse:8123/raw")

DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"


@dag(
    dag_id="dbt_build_banking",
    description="dbt build (snapshots + models + tests) after new data lands in raw",
    schedule=[RAW_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["banking", "dbt"],
)
def dbt_build_banking():
    BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_BIN} build",
        cwd=DBT_PROJECT_DIR,
        pool="clickhouse_pipeline",
    )


dbt_build_banking()