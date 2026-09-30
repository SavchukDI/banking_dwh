"""Проверка, что все DAG'и загружаются в образе Airflow без ошибок."""

import sys

from airflow.models import DagBag

EXPECTED_DAGS = {
    "minio_to_clickhouse_raw",
    "minio_to_clickhouse_raw_cosmos",
    "dbt_build_banking",
    "dbt_cosmos_banking",
}

dagbag = DagBag(include_examples=False)

if dagbag.import_errors:
    for path, error in dagbag.import_errors.items():
        print(f"❌ {path}\n{error}\n")
    sys.exit(1)

missing = EXPECTED_DAGS - set(dagbag.dag_ids)
if missing:
    print(f"❌ DAGs not found: {sorted(missing)}")
    sys.exit(1)

print(f"✅ {len(dagbag.dag_ids)} DAGs loaded: {sorted(dagbag.dag_ids)}")