import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL")
CONNECTOR_NAME = "postgres-connector"

connector_config = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    # Адрес Postgres с точки зрения контейнера connect
    "database.hostname": os.getenv("POSTGRES_HOST"),
    "database.port": os.getenv("POSTGRES_PORT"),
    "database.user": os.getenv("POSTGRES_USER"),
    "database.password": os.getenv("POSTGRES_PASSWORD"),
    "database.dbname": os.getenv("POSTGRES_DB"),
    "topic.prefix": "banking_server",
    "table.include.list": "public.customers,public.accounts,public.transactions",
    "plugin.name": "pgoutput",
    "slot.name": "banking_slot",
    "publication.name": "banking_publication",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
}


def wait_for_connect(retries: int = 30, delay: int = 5) -> None:
    for attempt in range(1, retries + 1):
        try:
            if requests.get(CONNECT_URL, timeout=5).status_code == 200:
                print("✅ Kafka Connect is up")
                return
        except requests.ConnectionError:
            pass
        print(f"⏳ Waiting for Kafka Connect ({attempt}/{retries})...")
        time.sleep(delay)
    print("❌ Kafka Connect is not available")
    sys.exit(1)


def upsert_connector() -> None:
    # PUT /connectors/{name}/config создаёт коннектор или обновляет конфиг существующего
    response = requests.put(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
        json=connector_config,
        timeout=10,
    )
    if response.status_code == 201:
        print("✅ Connector created")
    elif response.status_code == 200:
        print("🔄 Connector config updated")
    else:
        print(f"❌ Failed ({response.status_code}): {response.text}")
        sys.exit(1)


def print_status() -> None:
    time.sleep(3)
    status = requests.get(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10
    ).json()
    print(f"Connector: {status['connector']['state']}")
    for task in status.get("tasks", []):
        print(f"Task {task['id']}: {task['state']}")
        if task["state"] == "FAILED":
            print(task.get("trace", "")[:1000])


if __name__ == "__main__":
    wait_for_connect()
    upsert_connector()
    print_status()