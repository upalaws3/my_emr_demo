-- ===========================================================================
-- 02_gold_daily_txn_summary.sql  --  per account x date x currency (S6.4 Mart 2)
-- ===========================================================================
-- Table: ocbc_poc_dev_gold.daily_txn_summary  (Iceberg)
-- Grain: one row per account_id x txn_date x currency. Money aggregates are
-- kept PER CURRENCY (no FX conversion in the POC).
--
-- Columns (exact order, per S6.4):
--   account_id string, txn_date date, currency string, txn_count bigint,
--   debit_total decimal(18,2), credit_total decimal(18,2),
--   net_amount decimal(18,2)   (net_amount = credit_total - debit_total)
-- ===========================================================================

-- A) Recent daily activity, newest first.
SELECT
    account_id,
    txn_date,
    currency,
    txn_count,
    debit_total,
    credit_total,
    net_amount
FROM ocbc_poc_dev_gold.daily_txn_summary
ORDER BY txn_date DESC, account_id, currency
LIMIT 200;

-- B) Daily net flow by currency across all accounts (per-currency, no FX).
SELECT
    txn_date,
    currency,
    SUM(txn_count)    AS total_txns,
    SUM(debit_total)  AS total_debits,
    SUM(credit_total) AS total_credits,
    SUM(net_amount)   AS total_net
FROM ocbc_poc_dev_gold.daily_txn_summary
GROUP BY txn_date, currency
ORDER BY txn_date DESC, currency;

-- C) Top accounts by absolute net movement in a currency (example: SGD).
SELECT
    account_id,
    currency,
    SUM(net_amount) AS net_movement,
    SUM(txn_count)  AS txns
FROM ocbc_poc_dev_gold.daily_txn_summary
WHERE currency = 'SGD'
GROUP BY account_id, currency
ORDER BY ABS(SUM(net_amount)) DESC
LIMIT 25;
