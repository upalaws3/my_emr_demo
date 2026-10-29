-- ===========================================================================
-- 01_gold_customer_360.sql  --  one row per customer (design S6.4 Mart 1)
-- ===========================================================================
-- Table: ocbc_poc_dev_gold.customer_360  (Iceberg, auto-registered by the
-- canonical_to_gold Spark job). All name/email columns are ALREADY masked
-- (read from canonical, never silver).
--
-- Columns (exact order, per S6.4):
--   customer_id string, first_name_masked string, last_name_masked string,
--   email_masked string, country string, account_count int, card_count int,
--   active_account_count int, txn_count bigint, last_txn_at timestamp
-- ===========================================================================

-- A) Full customer 360 view, most-active customers first.
SELECT
    customer_id,
    first_name_masked,
    last_name_masked,
    email_masked,
    country,
    account_count,
    card_count,
    active_account_count,
    txn_count,
    last_txn_at
FROM ocbc_poc_dev_gold.customer_360
ORDER BY txn_count DESC, customer_id
LIMIT 100;

-- B) FinOps/engagement angle: customers per country with portfolio totals.
SELECT
    country,
    COUNT(*)                     AS customer_count,
    SUM(account_count)           AS total_accounts,
    SUM(active_account_count)    AS total_active_accounts,
    SUM(card_count)              AS total_cards,
    SUM(txn_count)               AS total_transactions
FROM ocbc_poc_dev_gold.customer_360
GROUP BY country
ORDER BY customer_count DESC;

-- C) Dormant customers (have accounts but no transactions) -- last_txn_at NULL.
SELECT
    customer_id,
    first_name_masked,
    last_name_masked,
    country,
    account_count,
    active_account_count
FROM ocbc_poc_dev_gold.customer_360
WHERE last_txn_at IS NULL
  AND account_count > 0
ORDER BY account_count DESC;
