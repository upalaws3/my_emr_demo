-- ===========================================================================
-- 03_gold_account_balances.sql  --  one row per account (design S6.4 Mart 3)
-- ===========================================================================
-- Table: ocbc_poc_dev_gold.account_balances  (Iceberg)
-- Grain: one row per account_id, balance in the account's NATIVE currency.
-- customer_token is the 16-char token of canonical customers.national_id
-- (joined via customer_id); the raw national id never reaches gold.
--
-- Columns (exact order, per S6.4):
--   account_id string, customer_id string, customer_token string,
--   account_type string, currency string, balance decimal(15,2), status string
-- ===========================================================================

-- A) All account balances, largest first.
SELECT
    account_id,
    customer_id,
    customer_token,
    account_type,
    currency,
    balance,
    status
FROM ocbc_poc_dev_gold.account_balances
ORDER BY balance DESC
LIMIT 100;

-- B) Balance totals by currency + status (per-currency; no cross-ccy sum).
SELECT
    currency,
    status,
    COUNT(*)      AS account_count,
    SUM(balance)  AS total_balance
FROM ocbc_poc_dev_gold.account_balances
GROUP BY currency, status
ORDER BY currency, status;

-- C) Portfolio per customer_token, per currency (tokenized PII join key).
SELECT
    customer_token,
    currency,
    COUNT(*)     AS account_count,
    SUM(balance) AS total_balance
FROM ocbc_poc_dev_gold.account_balances
GROUP BY customer_token, currency
ORDER BY total_balance DESC
LIMIT 50;
