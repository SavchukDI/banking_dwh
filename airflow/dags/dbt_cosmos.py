from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.datasets import Dataset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior

RAW_DATASET = Dataset("clickhouse://clickhouse:8123/raw/cosmos")

DBT_PROJECT_DIR = Path("/opt/airflow/banking_dbt")
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"

dbt_cosmos_banking = DbtDag(
    dag_id="dbt_cosmos_banking",
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=ProfileConfig(
        profile_name="banking_dbt",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
    ),
    execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        test_behavior=TestBehavior.AFTER_EACH,
        should_detach_multiple_parents_tests=True,
    ),
    operator_args={
        "install_deps": False,
        "emit_datasets": False,
    },
    schedule=[RAW_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["banking", "dbt", "cosmos"],
)