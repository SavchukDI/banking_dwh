-- Автообновление updated_at при любом UPDATE
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Клиенты
CREATE TABLE IF NOT EXISTS customers (
    customer_id    SERIAL PRIMARY KEY,
    first_name     VARCHAR(100) NOT NULL,
    last_name      VARCHAR(100) NOT NULL,
    email          VARCHAR(255) NOT NULL UNIQUE,
    phone          VARCHAR(30),
    date_of_birth  DATE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Счета
CREATE TABLE IF NOT EXISTS accounts (
    account_id     SERIAL PRIMARY KEY,
    customer_id    INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    account_type   VARCHAR(20) NOT NULL
                   CHECK (account_type IN ('checking', 'savings', 'credit')),
    currency       CHAR(3) NOT NULL DEFAULT 'RUB',
    balance        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    status         VARCHAR(20) NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'blocked', 'closed')),
    opened_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Транзакции
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      BIGSERIAL PRIMARY KEY,
    account_id          INT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    related_account_id  INT REFERENCES accounts(account_id),
    transaction_type    VARCHAR(20) NOT NULL
                        CHECK (transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment')),
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency            CHAR(3) NOT NULL DEFAULT 'RUB',
    status              VARCHAR(20) NOT NULL DEFAULT 'completed'
                        CHECK (status IN ('pending', 'completed', 'failed')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_related_account_id ON transactions(related_account_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Полная "before"-картинка строки в событиях UPDATE/DELETE для Debezium
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;