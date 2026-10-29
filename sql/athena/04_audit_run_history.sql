-- ===========================================================================
-- 04_audit_run_history.sql  --  pipeline run history (design S7.3)
-- ===========================================================================
-- Table: ocbc_poc_dev_audit.pipeline_runs  (Iceberg, unpartitioned)
-- Grain: one row per table per job. status is one of {SUCCESS, DQ_FAILED, FAILED}.
--
-- IMPORTANT: the per-table column is `table_name` -- NEVER `table` (a reserved
-- word in Athena/Trino). Every query below uses table_name.
--
-- Columns (DDL order, per S7.3):
--   run_id string, job string, layer_from string, layer_to string,
--   table_name string, status string, input_rows bigint, output_rows bigint,
--   rejected_rows bigint, dq_passed boolean, dq_critical_failed int,
--   started_at timestamp, ended_at timestamp, duration_ms bigint,
--   lineage_path string, quality_path string, emr_application_id string,
--   spark_app_id string
-- ===========================================================================

-- A) Full run history, most recent first.
SELECT
    run_id,
    job,
    layer_from,
    layer_to,
    table_name,
    status,
    input_rows,
    output_rows,
    rejected_rows,
    dq_passed,
    dq_critical_failed,
    started_at,
    ended_at,
    duration_ms,
    lineage_path,
    quality_path,
    emr_application_id,
    spark_app_id
FROM ocbc_poc_dev_audit.pipeline_runs
ORDER BY started_at DESC
LIMIT 200;

-- B) DQ gate failures: which run/table tripped the silver gate.
SELECT
    run_id,
    job,
    table_name,
    status,
    dq_passed,
    dq_critical_failed,
    input_rows,
    quality_path,
    started_at
FROM ocbc_poc_dev_audit.pipeline_runs
WHERE status = 'DQ_FAILED'
   OR dq_critical_failed > 0
ORDER BY started_at DESC;

-- C) Per-run, per-stage roll-up: row counts, rejects, and worst status.
SELECT
    run_id,
    job,
    layer_from,
    layer_to,
    COUNT(*)            AS tables_processed,
    SUM(input_rows)     AS total_input_rows,
    SUM(output_rows)    AS total_output_rows,
    SUM(rejected_rows)  AS total_rejected_rows,
    MAX(duration_ms)    AS max_stage_table_ms,
    MIN(CASE
            WHEN status = 'FAILED'    THEN 0
            WHEN status = 'DQ_FAILED' THEN 1
            ELSE 2
        END)            AS worst_status_rank  -- 0=FAILED, 1=DQ_FAILED, 2=SUCCESS
FROM ocbc_poc_dev_audit.pipeline_runs
GROUP BY run_id, job, layer_from, layer_to
ORDER BY run_id DESC, job;

-- D) Rejected-row watch (bronze reject contract): tables that dropped rows.
SELECT
    run_id,
    table_name,
    input_rows,
    output_rows,
    rejected_rows,
    started_at
FROM ocbc_poc_dev_audit.pipeline_runs
WHERE rejected_rows > 0
ORDER BY rejected_rows DESC, started_at DESC;
