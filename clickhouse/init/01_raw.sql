CREATE DATABASE IF NOT EXISTS raw;

-- -----------------------------
-- customers
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw.customers
(
    customer_id       Int32,
    first_name        String,
    last_name         String,
    email             String,
    phone             Nullable(String),
    date_of_birth     Nullable(Date32),
    created_at        DateTime64(6, 'UTC'),
    updated_at        DateTime64(6, 'UTC'),

    -- метаданные CDC
    _op               LowCardinality(String),
    _source_ts        DateTime64(3, 'UTC'),
    _lsn              UInt64,
    _kafka_partition  Int32,
    _kafka_offset     Int64,
    _source_file      String,
    _loaded_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(_source_ts)
ORDER BY (_kafka_partition, _kafka_offset);

-- -----------------------------
-- accounts
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw.accounts
(
    account_id        Int32,
    customer_id       Int32,
    account_type      LowCardinality(String),
    currency          LowCardinality(String),
    balance           Decimal(18, 2),
    status            LowCardinality(String),
    opened_at         DateTime64(6, 'UTC'),
    updated_at        DateTime64(6, 'UTC'),

    _op               LowCardinality(String),
    _source_ts        DateTime64(3, 'UTC'),
    _lsn              UInt64,
    _kafka_partition  Int32,
    _kafka_offset     Int64,
    _source_file      String,
    _loaded_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(_source_ts)
ORDER BY (_kafka_partition, _kafka_offset);

-- -----------------------------
-- transactions
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw.transactions
(
    transaction_id      Int64,
    account_id          Int32,
    related_account_id  Nullable(Int32),
    transaction_type    LowCardinality(String),
    amount              Decimal(18, 2),
    currency            LowCardinality(String),
    status              LowCardinality(String),
    created_at          DateTime64(6, 'UTC'),

    _op                 LowCardinality(String),
    _source_ts          DateTime64(3, 'UTC'),
    _lsn                UInt64,
    _kafka_partition    Int32,
    _kafka_offset       Int64,
    _source_file        String,
    _loaded_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(_source_ts)
ORDER BY (_kafka_partition, _kafka_offset);