import argparse
import os
import random
import sys
import time
from decimal import ROUND_DOWN, Decimal

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

# -----------------------------
# Конфигурация
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = Decimal("100000.00")
CURRENCY = "RUB"

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("100000.00")

# Изменения существующих записей (дают UPDATE-события для CDC и версии для SCD2)
NUM_CUSTOMER_UPDATES = 3
NUM_ACCOUNT_STATUS_UPDATES = 2

DEFAULT_LOOP = True
SLEEP_SECONDS = 2

parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Хелперы
# -----------------------------
fake = Faker("ru_RU")


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# -----------------------------
# Подключение к Postgres
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
# autocommit выключен: каждую бизнес-операцию оборачиваем в транзакцию через `with conn:`
cur = conn.cursor()


# -----------------------------
# Вставка новых записей
# -----------------------------
def create_customers() -> list[int]:
    customer_ids = []
    for _ in range(NUM_CUSTOMERS):
        cur.execute(
            """
            INSERT INTO customers (first_name, last_name, email, phone, date_of_birth)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING customer_id
            """,
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number(),
                fake.date_of_birth(minimum_age=18, maximum_age=80),
            ),
        )
        row = cur.fetchone()
        if row:  # None, если такой email уже был в базе с прошлых запусков
            customer_ids.append(row[0])
    return customer_ids


def create_accounts(customer_ids: list[int]) -> list[int]:
    account_ids = []
    for customer_id in customer_ids:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            cur.execute(
                """
                INSERT INTO accounts (customer_id, account_type, currency, balance)
                VALUES (%s, %s, %s, %s)
                RETURNING account_id
                """,
                (
                    customer_id,
                    random.choice(["checking", "savings"]),
                    CURRENCY,
                    random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
                ),
            )
            account_ids.append(cur.fetchone()[0])
    return account_ids


def get_active_accounts() -> list[int]:
    cur.execute("SELECT account_id FROM accounts WHERE status = 'active'")
    return [row[0] for row in cur.fetchall()]


def create_transaction(active_accounts: list[int]) -> str | None:
    account_id = random.choice(active_accounts)
    txn_type = random.choice(["deposit", "withdrawal", "transfer", "payment"])
    amount = random_money(Decimal("1.00"), MAX_TXN_AMOUNT)

    related_account_id = None
    if txn_type == "transfer":
        others = [a for a in active_accounts if a != account_id]
        if not others:
            return None
        related_account_id = random.choice(others)

    if txn_type == "deposit":
        cur.execute(
            "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
            (amount, account_id),
        )
        status = "completed"
    else:
        # Списание: FOR UPDATE блокирует строку счёта до конца транзакции
        cur.execute(
            "SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE",
            (account_id,),
        )
        balance = cur.fetchone()[0]

        if balance >= amount:
            cur.execute(
                "UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
                (amount, account_id),
            )
            if txn_type == "transfer":
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                    (amount, related_account_id),
                )
            status = "completed"
        else:
            status = "failed"  # недостаточно средств: транзакцию пишем, баланс не трогаем

    cur.execute(
        """
        INSERT INTO transactions
            (account_id, related_account_id, transaction_type, amount, currency, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (account_id, related_account_id, txn_type, amount, CURRENCY, status),
    )
    return status


# -----------------------------
# Изменения существующих записей
# -----------------------------
def update_customers() -> int:
    cur.execute(
        "SELECT customer_id FROM customers ORDER BY random() LIMIT %s",
        (NUM_CUSTOMER_UPDATES,),
    )
    rows = cur.fetchall()
    for (customer_id,) in rows:
        if random.random() < 0.5:
            cur.execute(
                "UPDATE customers SET phone = %s WHERE customer_id = %s",
                (fake.phone_number(), customer_id),
            )
        else:
            cur.execute(
                "UPDATE customers SET last_name = %s WHERE customer_id = %s",
                (fake.last_name(), customer_id),
            )
    return len(rows)


def update_account_statuses() -> int:
    cur.execute(
        """
        SELECT account_id, status
        FROM accounts
        WHERE status <> 'closed'
        ORDER BY random()
        LIMIT %s
        """,
        (NUM_ACCOUNT_STATUS_UPDATES,),
    )
    rows = cur.fetchall()
    for account_id, status in rows:
        if status == "active":
            new_status = random.choices(["blocked", "closed"], weights=[3, 1])[0]
        else:  # blocked -> active
            new_status = "active"
        cur.execute(
            "UPDATE accounts SET status = %s WHERE account_id = %s",
            (new_status, account_id),
        )
    return len(rows)


# -----------------------------
# Одна итерация
# -----------------------------
def run_iteration():
    with conn:
        customer_ids = create_customers()
        account_ids = create_accounts(customer_ids)

    with conn:
        active_accounts = get_active_accounts()

    stats = {"completed": 0, "failed": 0}
    for _ in range(NUM_TRANSACTIONS):
        with conn:  # каждая денежная операция — отдельная транзакция БД
            status = create_transaction(active_accounts)
        if status:
            stats[status] += 1

    with conn:
        n_customers_upd = update_customers()
        n_accounts_upd = update_account_statuses()

    print(
        f"✅ Inserted: {len(customer_ids)} customers, {len(account_ids)} accounts, "
        f"{stats['completed']} completed / {stats['failed']} failed transactions. "
        f"Updated: {n_customers_upd} customers, {n_accounts_upd} account statuses."
    )


# -----------------------------
# Основной цикл
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)