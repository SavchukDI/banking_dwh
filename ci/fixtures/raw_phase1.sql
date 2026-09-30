-- =====================================================================
-- Фаза 1: начальный снапшот + изменения + дубли + удаление
-- =====================================================================

-- ---------------- customers ----------------
INSERT INTO raw.customers
    (customer_id, first_name, last_name, email, phone, date_of_birth, created_at, updated_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    -- снапшот
    (1, 'Ivan', 'Petrov',   'ivan@example.com', '+7 900 000 00 01', '1985-05-10', '2026-01-01 10:00:00', '2026-01-01 10:00:00', 'r', '2026-01-01 12:00:00', 100, 0, 0, 'ci/phase1'),
    (2, 'Anna', 'Sidorova', 'anna@example.com', '+7 900 000 00 02', '1990-03-15', '2026-01-01 10:05:00', '2026-01-01 10:05:00', 'r', '2026-01-01 12:00:00', 100, 0, 1, 'ci/phase1'),
    -- дубль предыдущего события (тот же partition + offset)
    (2, 'Anna', 'Sidorova', 'anna@example.com', '+7 900 000 00 02', '1990-03-15', '2026-01-01 10:05:00', '2026-01-01 10:05:00', 'r', '2026-01-01 12:00:00', 100, 0, 1, 'ci/phase1'),
    -- новый клиент без телефона и даты рождения
    (3, 'Oleg', 'Smirnov',  'oleg@example.com', NULL, NULL, '2026-01-02 09:00:00', '2026-01-02 09:00:00', 'c', '2026-01-02 09:00:00', 200, 0, 2, 'ci/phase1'),
    -- смена телефона клиента 1
    (1, 'Ivan', 'Petrov',   'ivan@example.com', '+7 900 000 00 99', '1985-05-10', '2026-01-01 10:00:00', '2026-01-02 10:00:00', 'u', '2026-01-02 10:00:00', 210, 0, 3, 'ci/phase1'),
    -- удаление клиента 3 (данные строки — из before)
    (3, 'Oleg', 'Smirnov',  'oleg@example.com', NULL, NULL, '2026-01-02 09:00:00', '2026-01-02 09:00:00', 'd', '2026-01-02 11:00:00', 220, 0, 4, 'ci/phase1');

-- ---------------- accounts ----------------
INSERT INTO raw.accounts
    (account_id, customer_id, account_type, currency, balance, status, opened_at, updated_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    -- снапшот
    (10, 1, 'checking', 'RUB', 1000.00, 'active', '2026-01-01 10:10:00', '2026-01-01 10:10:00', 'r', '2026-01-01 12:00:00', 100, 0, 0, 'ci/phase1'),
    (11, 2, 'savings',  'RUB',  500.00, 'active', '2026-01-01 10:15:00', '2026-01-01 10:15:00', 'r', '2026-01-01 12:00:00', 100, 0, 1, 'ci/phase1'),
    -- два изменения баланса счёта 10, вставлены НЕ по порядку:
    -- более позднее (lsn 310) записано раньше более раннего (lsn 300)
    (10, 1, 'checking', 'RUB', 1100.00, 'active', '2026-01-01 10:10:00', '2026-01-02 12:00:00', 'u', '2026-01-02 12:00:00', 310, 0, 3, 'ci/phase1'),
    (10, 1, 'checking', 'RUB', 1200.00, 'active', '2026-01-01 10:10:00', '2026-01-02 11:00:00', 'u', '2026-01-02 11:00:00', 300, 0, 2, 'ci/phase1');

-- ---------------- transactions ----------------
INSERT INTO raw.transactions
    (transaction_id, account_id, related_account_id, transaction_type, amount, currency, status, created_at,
     _op, _source_ts, _lsn, _kafka_partition, _kafka_offset, _source_file)
VALUES
    (1000, 10, NULL, 'deposit',    200.00, 'RUB', 'completed', '2026-01-02 11:00:00', 'c', '2026-01-02 11:00:00', 300, 0, 0, 'ci/phase1'),
    (1001, 10, NULL, 'withdrawal', 100.00, 'RUB', 'completed', '2026-01-02 12:00:00', 'c', '2026-01-02 12:00:00', 310, 0, 1, 'ci/phase1'),
    -- неуспешная операция (недостаточно средств)
    (1002, 11, NULL, 'withdrawal', 900.00, 'RUB', 'failed',    '2026-01-02 13:00:00', 'c', '2026-01-02 13:00:00', 320, 0, 2, 'ci/phase1'),
    -- дубль
    (1002, 11, NULL, 'withdrawal', 900.00, 'RUB', 'failed',    '2026-01-02 13:00:00', 'c', '2026-01-02 13:00:00', 320, 0, 2, 'ci/phase1');