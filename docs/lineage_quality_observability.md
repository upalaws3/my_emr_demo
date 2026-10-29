# Lineage, Quality & Observability

The customer wants to see where data came from, prove it is trustworthy, and
observe the pipeline running. The baseline covers all three with lightweight,
self-contained mechanisms — no external lineage/DQ platforms.

## Custom JSON lineage

Each stage emits **one lineage JSON per table** to
`s3://<datalake>/lineage/<run_id>/<job>/<table>.json`
(`src/common/lineage.py`). The document captures inputs → outputs and the
column-level transforms applied. Shape (design §7.1):

```json
{
  "schema_version": "1.0",
  "run_id": "2025-01-01T10-00-00Z_ab12cd",
  "job": "bronze_to_silver",
  "table": "customers",
  "layer_from": "bronze",
  "layer_to": "silver",
  "started_at": "2025-01-01T10:00:00Z",
  "ended_at": "2025-01-01T10:00:42Z",
  "status": "SUCCESS",
  "inputs":  [{ "dataset": "s3://.../bronze/customers/", "format": "parquet",
               "row_count": 100, "glue_table": "ocbc_poc_dev_bronze.customers" }],
  "outputs": [{ "dataset": "glue_catalog.ocbc_poc_dev_silver.customers",
               "format": "iceberg", "row_count": 100,
               "glue_table": "ocbc_poc_dev_silver.customers" }],
  "column_mappings": [{ "output": "email", "inputs": ["email"], "transform": "trim" }],
  "metrics": { "input_rows": 100, "output_rows": 100, "rejected_rows": 0 }
}
```

Notes:
- `glue_table` is **present for cataloged datasets** (bronze/silver/canonical/
  gold) and **omitted for uncataloged ones** — specifically the landing input of
  `landing_to_bronze`. A reader treats a missing `glue_table` as "uncataloged".
- Canonical lineage carries `transform = "mask:<method>"` on each PII column;
  gold lineage carries aggregation transforms (e.g. `net_amount` ←
  `[transactions.amount, transactions.txn_type]`, `transform = "agg:sum"`).
- Lineage is registered to S3/Glue as JSON — the deliberately lightweight choice
  for a POC (OpenLineage/Marquez is out of scope).

## Data quality + the FAIL gate

DQ rules are declared per table in `config/pipeline.yaml` under `dq_rules`, each
with an explicit `severity` (`critical` | `warn`) so a demo can retune gating
without code changes. The `bronze_to_silver` job owns the gate and runs, per
table, the fixed sequence (design §6.2a):

```
run_expectations(...)      # evaluate all expectations
emit_quality(result)       # ALWAYS write the quality JSON first
enforce_gate(result)       # raise QualityGateError iff a critical check failed
```

A **critical** failure halts the pipeline at silver: the job writes a
`DQ_FAILED` audit row for the failing table, re-raises, and exits non-zero. The
Step Functions `Catch [States.ALL]` routes to `PipelineFailed` and the run stops
before canonical. The quality JSON
(`quality/<run_id>/<job>/<table>.json`, design §7.2) records every expectation's
result and `passed`/`critical_failed`/`warn_failed` so the failure stays
diagnosable out-of-band.

Baseline rules (per table): PK not-null/unique, referential FK checks (to the
just-written silver parent), value-in-set checks, range checks, and
`row_count ≥ 1`. **`row_count ≥ 1` is critical only for `transactions`** (an
empty fact table makes gold meaningless) and `warn` for the other three. The
`--inject-dq-failure` generator flag deterministically trips the
`transaction_id_unique` critical check for demos.

## Structured logging + the audit table

- **Logging** (`src/common/logging_util.py`): single-line JSON to stdout, so on
  EMR Serverless the driver/executor logs land in CloudWatch
  (`/aws/emr-serverless/ocbc-poc-dev`) alongside the Step Functions execution
  history (`/aws/vendedlogs/states/ocbc-poc-dev-medallion-pipeline`). A `timed`
  context manager logs `duration_ms` per event.
- **Audit table** (`src/common/audit.py`): one durable row per table per job in
  Iceberg `ocbc_poc_dev_audit.pipeline_runs` (+ a JSON mirror under `audit/`).
  Columns (design §7.3) include `run_id`, `job`, `layer_from`/`layer_to`,
  **`table_name`** (never `table`), `status` (`SUCCESS` | `DQ_FAILED` |
  `FAILED`), row counts, `dq_passed`/`dq_critical_failed`, timing,
  `lineage_path`/`quality_path`, and the EMR/Spark app ids. The table is
  **unpartitioned** (a handful of rows per run at POC scale) for clean Athena v3
  compatibility.

## Querying the trail

`sql/athena/04_audit_run_history.sql` reads `pipeline_runs` (always via
`table_name`) to show run history, DQ failures, per-run roll-ups, and a
rejected-row watch. Because observability side-effects (lineage emit, Glue
register, audit write) are **best-effort** (design §10), a telemetry hiccup logs
an event but never blocks the data path; the DQ gate, reads, and writes stay
fatal.
