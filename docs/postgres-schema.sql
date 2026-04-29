-- ================================================================
-- PostgreSQL schema for the Loan Eligibility Flink demo
-- ================================================================
-- Run once against your database before starting the Flink job.
--
-- Local Docker:
--   psql -h localhost -U flink -d loan_db -f docs/postgres-schema.sql
--
-- Neon / Supabase / any hosted Postgres:
--   Paste the SQL below into the SQL editor in your provider's console.
-- ================================================================

CREATE TABLE IF NOT EXISTS loan_eligibility (
    id                      BIGSERIAL       PRIMARY KEY,
    customer_id             VARCHAR(50)     NOT NULL,
    account_id              VARCHAR(50)     NOT NULL,
    transaction_id          VARCHAR(50)     NOT NULL,
    transaction_amount_gbp  NUMERIC(12, 2)  NOT NULL,
    transaction_currency    VARCHAR(10)     NOT NULL,
    eligibility_status      VARCHAR(20)     NOT NULL,   -- ELIGIBLE | NOT_ELIGIBLE
    eligibility_reason      VARCHAR(255)    NOT NULL,
    loan_rule_version       VARCHAR(20)     NOT NULL,
    transaction_time        TIMESTAMPTZ     NOT NULL,
    processed_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for customer-centric queries
CREATE INDEX IF NOT EXISTS idx_loan_eligibility_customer_id
    ON loan_eligibility (customer_id);

-- Index for deduplication / idempotency checks
CREATE INDEX IF NOT EXISTS idx_loan_eligibility_transaction_id
    ON loan_eligibility (transaction_id);

-- Index to support time-range queries / auditing
CREATE INDEX IF NOT EXISTS idx_loan_eligibility_processed_at
    ON loan_eligibility (processed_at DESC);

-- ================================================================
-- Useful verification queries
-- ================================================================

-- Check all eligibility decisions
-- SELECT * FROM loan_eligibility ORDER BY processed_at DESC LIMIT 20;

-- Summary by status
-- SELECT eligibility_status, COUNT(*) FROM loan_eligibility GROUP BY eligibility_status;

-- All eligible customers
-- SELECT customer_id, transaction_id, transaction_amount_gbp, processed_at
-- FROM loan_eligibility
-- WHERE eligibility_status = 'ELIGIBLE'
-- ORDER BY processed_at DESC;
