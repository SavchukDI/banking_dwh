from datetime import datetime, timedelta, timezone

import boto3
import clickhouse_connect
import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

TABLES = ["customers", "accounts", "transactions"]
LOOKBACK_DAYS = 2  # сколько последних дневных партиций MinIO просматривать
RAW_DATASET = Dataset("clickhouse://clickhouse:8123/raw") # Событие "в raw появились новые данные" — на него подписан DAG dbt

# -----------------------------
# Маппинг Parquet -> raw-таблица ClickHouse
# -----------------------------
META_COLUMNS = ["_op", "_source_ts", "_lsn", "_kafka_partition", "_kafka_offset", "_source_file"]
META_SELECT = """
    _op,
    fromUnixTimestamp64Milli(_source_ts_ms, 'UTC'),
    _lsn,
    _kafka_partition,
    _kafka_offset,
    _path
"""

TABLE_CONFIG = {
    "customers": {
        "columns": [
            "customer_id", "first_name", "last_name", "email", "phone",
            "date_of_birth", "created_at", "updated_at",
        ],
        "select": """
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            addDays(toDate32('1970-01-01'), date_of_birth),
            parseDateTime64BestEffort(created_at, 6, 'UTC'),
            parseDateTime64BestEffort(updated_at, 6, 'UTC')
        """,
    },
    "accounts": {
        "columns": [
            "account_id", "customer_id", "account_type", "currency", "balance",
            "status", "opened_at", "updated_at",
        ],
        "select": """
            account_id,
            customer_id,
            account_type,
            currency,
            CAST(balance AS Decimal(18, 2)),
            status,
            parseDateTime64BestEffort(opened_at, 6, 'UTC'),
            parseDateTime64BestEffort(updated_at, 6, 'UTC')
        """,
    },
    "transactions": {
        "columns": [
            "transaction_id", "account_id", "related_account_id", "transaction_type",
            "amount", "currency", "status", "created_at",
        ],
        "select": """
            transaction_id,
            account_id,
            related_account_id,
            transaction_type,
            CAST(amount AS Decimal(18, 2)),
            currency,
            status,
            parseDateTime64BestEffort(created_at, 6, 'UTC')
        """,
    },
}


# -----------------------------
# Хелперы
# -----------------------------
def get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse")
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
    )


def get_minio():
    conn = BaseHook.get_connection("minio")
    endpoint = f"http://{conn.host}:{conn.port}"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    return s3, conn, endpoint


def list_files(s3, bucket: str, table: str) -> list[str]:
    """Ключи Parquet-файлов таблицы за последние LOOKBACK_DAYS дней (с пагинацией)."""
    today = datetime.now(timezone.utc).date()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for days_ago in range(LOOKBACK_DAYS):
        day = today - timedelta(days=days_ago)
        prefix = f"{table}/date={day:%Y-%m-%d}/"
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


def get_loaded_files(client, table: str) -> set[str]:
    result = client.query(
        "SELECT file_path FROM raw._load_log WHERE table_name = {table:String}",
        parameters={"table": table},
    )
    return {row[0] for row in result.result_rows}


def build_insert_sql(table: str, file_url: str, access_key: str, secret_key: str) -> str:
    cfg = TABLE_CONFIG[table]
    columns = ", ".join(cfg["columns"] + META_COLUMNS)
    return f"""
        INSERT INTO raw.{table} ({columns})
        SELECT
            {cfg['select']},
            {META_SELECT}
        FROM s3('{file_url}', '{access_key}', '{secret_key}', 'Parquet')
    """


# -----------------------------
# Задача
# -----------------------------
@task(outlets=[RAW_DATASET], pool="clickhouse_pipeline")
def load_table(table: str) -> int:
    s3, minio_conn, endpoint = get_minio()
    bucket = minio_conn.schema
    client = get_clickhouse_client()

    loaded = get_loaded_files(client, table)
    # _path в ClickHouse имеет вид "bucket/key", поэтому сравниваем в том же формате
    new_keys = [k for k in list_files(s3, bucket, table) if f"{bucket}/{k}" not in loaded]

    if not new_keys:
        # skipped-задача не отправляет событие Dataset -> dbt не запустится впустую
        raise AirflowSkipException(f"[{table}] no new files")
        #print(f"[{table}] no new files")
        #return 0

    total_rows = 0
    for key in new_keys:
        sql = build_insert_sql(
            table,
            file_url=f"{endpoint}/{bucket}/{key}",
            access_key=minio_conn.login,
            secret_key=minio_conn.password,
        )
        summary = client.command(sql)
        rows = int(getattr(summary, "written_rows", 0))

        # Журнал пишем только после успешного INSERT
        client.insert(
            "raw._load_log",
            [[table, f"{bucket}/{key}", rows]],
            column_names=["table_name", "file_path", "rows_loaded"],
        )
        total_rows += rows
        print(f"[{table}] {key}: {rows} rows")

    print(f"[{table}] loaded {len(new_keys)} files, {total_rows} rows")
    return total_rows


# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="minio_to_clickhouse_raw",
    description="Load CDC Parquet files from MinIO into ClickHouse raw layer",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["banking", "raw"],
)
def minio_to_clickhouse_raw():
    for table in TABLES:
        load_table.override(task_id=f"load_{table}")(table)


minio_to_clickhouse_raw()