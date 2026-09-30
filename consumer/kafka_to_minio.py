import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# -----------------------------
# Конфигурация
# -----------------------------
TOPIC_PREFIX = "banking_server.public."
TABLES = ["customers", "accounts", "transactions"]
TOPICS = [TOPIC_PREFIX + t for t in TABLES]

BATCH_SIZE = 500
FLUSH_INTERVAL_SEC = 30
BUCKET = os.getenv("MINIO_BUCKET")

# -----------------------------
# Явные схемы Parquet
# -----------------------------
# Метаданные CDC-события — одинаковые для всех таблиц
META_FIELDS = [
    ("_op", pa.string()),              # r / c / u / d
    ("_source_ts_ms", pa.int64()),     # время изменения в Postgres
    ("_lsn", pa.int64()),              # позиция в WAL
    ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()),
]

SCHEMAS = {
    "customers": pa.schema([
        ("customer_id", pa.int32()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("date_of_birth", pa.int32()),   # дни с 1970-01-01 (так Debezium кодирует DATE)
        ("created_at", pa.string()),     # ISO-8601 строка
        ("updated_at", pa.string()),
        *META_FIELDS,
    ]),
    "accounts": pa.schema([
        ("account_id", pa.int32()),
        ("customer_id", pa.int32()),
        ("account_type", pa.string()),
        ("currency", pa.string()),
        ("balance", pa.string()),        # decimal.handling.mode = string
        ("status", pa.string()),
        ("opened_at", pa.string()),
        ("updated_at", pa.string()),
        *META_FIELDS,
    ]),
    "transactions": pa.schema([
        ("transaction_id", pa.int64()),
        ("account_id", pa.int32()),
        ("related_account_id", pa.int32()),
        ("transaction_type", pa.string()),
        ("amount", pa.string()),
        ("currency", pa.string()),
        ("status", pa.string()),
        ("created_at", pa.string()),
        *META_FIELDS,
    ]),
}

# -----------------------------
# Клиенты
# -----------------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    group_id=os.getenv("KAFKA_GROUP"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # коммитим offset'ы только после записи в MinIO
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
)

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)


def ensure_bucket() -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"🪣 Created bucket {BUCKET}")


# -----------------------------
# Разбор события Debezium
# -----------------------------
def to_row(message) -> dict | None:
    payload = (message.value or {}).get("payload")
    if not payload:
        return None

    op = payload.get("op")
    # У DELETE данные строки лежат в before (after = null)
    data = payload.get("before") if op == "d" else payload.get("after")
    if data is None:
        return None  # например, TRUNCATE — у него нет ни before, ни after

    source = payload.get("source", {})
    return {
        **data,
        "_op": op,
        "_source_ts_ms": source.get("ts_ms"),
        "_lsn": source.get("lsn"),
        "_kafka_partition": message.partition,
        "_kafka_offset": message.offset,
    }


# -----------------------------
# Запись в MinIO
# -----------------------------
def write_batch(table: str, rows: list[dict]) -> None:
    arrow_table = pa.Table.from_pylist(rows, schema=SCHEMAS[table])

    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer)

    now = datetime.now(timezone.utc)
    key = f"{table}/date={now:%Y-%m-%d}/{table}_{now:%H%M%S%f}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"✅ {len(rows)} rows -> s3://{BUCKET}/{key}")


buffers: dict[str, list[dict]] = {t: [] for t in TABLES}


def flush_all() -> None:
    for table, rows in buffers.items():
        if rows:
            write_batch(table, rows)
            rows.clear()
    consumer.commit()  # только после успешной записи всех файлов


# -----------------------------
# Основной цикл
# -----------------------------
ensure_bucket()
print("✅ Connected to Kafka. Listening for messages...")

last_flush = time.monotonic()
try:
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        for tp, messages in records.items():
            table = tp.topic.removeprefix(TOPIC_PREFIX)
            for message in messages:
                row = to_row(message)
                if row:
                    buffers[table].append(row)

        total = sum(len(rows) for rows in buffers.values())
        timed_out = time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC
        if total >= BATCH_SIZE or (total > 0 and timed_out):
            flush_all()
            last_flush = time.monotonic()

except KeyboardInterrupt:
    print("\nInterrupted. Flushing buffers...")
    flush_all()

finally:
    consumer.close()