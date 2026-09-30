-- =====================================================================
-- Фаза 2: изменения между двумя запусками dbt (проверка SCD2 и инкремента)
-- =====================================================================

-- Клиент 2 сменил фамилию в 07:00 — до перевода в 08:00
INSERT INTO raw.customers
    (customer_id, first_name, last_name, email, phone, date_of_birth, created_at, updated_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    (2, 'Anna', 'Volkova', 'anna@example.com', '+7 900 000 00 02', '1990-03-15', '2026-01-01 10:05:00', '2026-01-03 07:00:00', 'u', '2026-01-03 07:00:00', 380, 0, 5, 'ci/phase2');

-- Перевод 50.00 со счёта 11 на счёт 10
INSERT INTO raw.transactions
    (transaction_id, account_id, related_account_id, transaction_type, amount, currency, status, created_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    (1003, 11, 10, 'transfer', 50.00, 'RUB', 'completed', '2026-01-03 08:00:00', 'c', '2026-01-03 08:00:00', 390, 0, 3, 'ci/phase2');

-- Балансы после перевода: НЕ должны создать новых версий в снапшоте
INSERT INTO raw.accounts
    (account_id, customer_id, account_type, currency, balance, status, opened_at, updated_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    (11, 2, 'savings',  'RUB',  450.00, 'active', '2026-01-01 10:15:00', '2026-01-03 08:00:00', 'u', '2026-01-03 08:00:00', 391, 0, 4, 'ci/phase2'),
    (10, 1, 'checking', 'RUB', 1150.00, 'active', '2026-01-01 10:10:00', '2026-01-03 08:00:00', 'u', '2026-01-03 08:00:00', 392, 0, 5, 'ci/phase2');

-- Блокировка счёта 11: должна создать новую версию
INSERT INTO raw.accounts
    (account_id, customer_id, account_type, currency, balance, status, opened_at, updated_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    (11, 2, 'savings', 'RUB', 450.00, 'blocked', '2026-01-01 10:15:00', '2026-01-03 09:00:00', 'u', '2026-01-03 09:00:00', 400, 0, 6, 'ci/phase2');