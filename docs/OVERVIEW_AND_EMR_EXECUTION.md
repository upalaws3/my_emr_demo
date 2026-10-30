# OCBC EMR Medallion Pipeline — Overview & EMR Execution Guide

*A two-page summary of what was built and how to run the pipeline on AWS EMR Serverless. Region: `us-east-1`.*

---

## Page 1 — What has been built

### The one-line summary
A POC-scale, end-to-end banking data pipeline that lands four source tables in S3, refines them through a medallion architecture (landing → bronze → silver → canonical → gold) on **EMR Serverless (PySpark)**, enforces **data quality**, captures **lineage** and an **audit trail**, **masks PII**, and exposes business marts for **Athena** — all provisioned by **Terraform** and orchestrated by **Step Functions**.

### Architecture at a glance

```
            ┌─────────┐   ┌────────┐   ┌────────┐    ┌───────────┐   ┌──────┐
 S3 landing │ LANDING │ → │ BRONZE │ → │ SILVER │ →  │ CANONICAL │ → │ GOLD │ → Athena
 CSV/JSON   └─────────┘   └────────┘   └────────┘    └───────────┘   └──────┘
   4 tables   raw files    Parquet      Iceberg         Iceberg       Iceberg
                           (typed)     (DQ GATE)      (PII masked)    (marts)
                                           │
                                   critical DQ fail → pipeline halts
```

| Layer | Format | What happens |
| --- | --- | --- |
| **Landing** | CSV / JSON | Raw synthetic files for `customers`, `accounts`, `cards`, `transactions`. |
| **Bronze** | Parquet | Typed ingest; uncastable values routed to a `_rejected/` side-path. |
| **Silver** | Iceberg | Cleansed + validated. **Owns the data-quality FAIL gate** — a critical failure stops the whole pipeline here. |
| **Canonical** | Iceberg | Standardized/conformed, with **PII masking/tokenization** applied. |
| **Gold** | Iceberg | Three business marts: `customer_360`, `daily_txn_summary`, `account_balances`. |

### The four PySpark jobs (`src/jobs/`)
1. `landing_to_bronze.py` — read raw files, apply typed schema, write bronze Parquet.
2. `bronze_to_silver.py` — run data-quality expectations and **enforce the gate**, then write silver Iceberg.
3. `silver_to_canonical.py` — apply the PII masking catalog, write canonical Iceberg.
4. `canonical_to_gold.py` — build the three aggregate marts, write gold Iceberg.

All four share one CLI contract: `--run-id <id> --env dev --all` (the job name is derived from the script filename; there is no `--job`).

### Cross-cutting capabilities (`src/common/`)
- **Data quality** (`quality.py`) — not-null, uniqueness, referential integrity, value ranges, allowed sets, row-count thresholds. Each rule has a `critical` or `warn` severity; critical failures halt the pipeline. Results written as JSON to S3.
- **Lineage** (`lineage.py`) — lightweight custom JSON per job run (inputs, outputs, column mappings, row counts, run id, timestamp) written to S3 and registered in the Glue Data Catalog.
- **Audit/metrics** (`audit.py`) — a run-level audit row per stage (job, layer, status, row counts, duration) to an S3 audit prefix and a queryable audit table.
- **PII masking** (`masking.py`) — names, email, phone, card PAN, and `national_id` tokenized (SHA-256 + salt) so canonical/gold consumers never see raw PII.
- **Structured logging** (`logging_util.py`) — consistent, greppable events with run id, layer, table, row counts, and timings.
- **Config** (`config_loader.py`) — single source of truth `config/pipeline.yaml`, validated fail-fast.

### Banking value-adds
- **Security** — SSE-KMS encryption on all S3 (customer-managed key), block-public-access, least-privilege IAM roles for EMR and Step Functions.
- **FinOps** — EMR Serverless auto-stop (idle cost → zero), S3 lifecycle rules on transient data, cost-allocation tags on every resource via Terraform `default_tags`.
- **Tracking** — the audit table is the run-tracking surface; lineage + quality JSON give full traceability.

### What's in the repo
| Path | Contents |
| --- | --- |
| `config/pipeline.yaml` | Single source of truth — names, region, salt, reference data, masking catalog, DQ rules. |
| `src/common/`, `src/jobs/` | Shared modules + the four stage jobs. |
| `data/generator/`, `scripts/` | Synthetic data generator + upload / local-check helpers. |
| `infra/terraform/` | All AWS resources + the Step Functions ASL template. |
| `sql/athena/` | Four example gold/audit queries. |
| `docs/`, `README.md`, `INSTALL.md` | Architecture, FinOps, security, lineage, PII catalog, and the step-by-step install guide. |

### Verified locally (no AWS on the authoring host)
`compileall` clean · data generator emits the 4 files with PII populated · `pytest` 44 passed · `run_local_checks.py` OK. Terraform, EMR/Spark runtime, Step Functions, and Athena are **validated by deploying to AWS** (not runnable on the bare dev host).

---

## Page 2 — How to execute the pipeline on EMR

> Prerequisite: the infrastructure is deployed with Terraform (`terraform apply` in `infra/terraform/`) from a machine with AWS credentials in **`us-east-1`**. See `INSTALL.md` sections 1–5 for the one-time setup. The steps below assume `terraform apply` has succeeded.

Orchestration model: a **Step Functions** state machine runs the four stages in sequence, each as an **EMR Serverless `startJobRun.sync`** task. You do not launch EMR jobs by hand — you start ONE state-machine execution and it drives all four stages. Every stage catches any error and routes to a single shared `PipelineFailed` state; success ends at `PipelineSucceeded`.

### Step 1 — Grab the deployment outputs
From `infra/terraform/`:
```bash
terraform output
# key values you'll use:
terraform output -raw state_machine_arn      # the pipeline to start
terraform output -raw datalake_bucket        # where code + data live
terraform output -raw emr_application_id      # the EMR Serverless app
terraform output -raw athena_workgroup        # for querying gold
```

### Step 2 — Publish the job code to S3
EMR Serverless runs the Spark scripts from the S3 path the state machine's `entryPoint` expects (`jobs_s3_base`). Sync the code once (and after any job change):
```bash
aws s3 sync ../../src "s3://$(terraform output -raw datalake_bucket)/src" --region us-east-1
```

### Step 3 — Generate and upload the landing data
```bash
# from the repo root, produce the 4 sample files
python -m data.generator.generate --out ./_local_out

# upload them to the landing prefix (account id is part of the bucket name)
python scripts/upload_landing.py --src ./_local_out --env dev --account-id <YOUR_ACCOUNT_ID>
```

### Step 4 — Start the pipeline (one execution, four EMR stages)
**Option A — CLI:**
```bash
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --name "demo-$(date +%Y%m%d-%H%M%S)" \
  --region us-east-1
```
**Option B — Console:** Step Functions → State machines → `ocbc-poc-dev-medallion-pipeline` → **Start execution**.

The execution name becomes the pipeline **run id**, which flows into every EMR job (`--run-id`), every lineage/quality JSON path, and every audit row — so one execution is fully traceable end to end.

### Step 5 — Watch it run
- **Step Functions console (`us-east-1`):** the graph shows
  `LandingToBronze → BronzeToSilver → SilverToCanonical → CanonicalToGold`.
  A critical DQ failure in `BronzeToSilver` routes to `PipelineFailed` and the run stops *before* canonical/gold.
- **EMR Serverless console:** each stage appears as a job run under the `ocbc-poc-dev` application, with Spark driver/executor logs.
- **CloudWatch Logs (`us-east-1`):**
  - Step Functions: `/aws/vendedlogs/states/ocbc-poc-dev-medallion-pipeline`
  - EMR Serverless: `/aws/emr-serverless/ocbc-poc-dev`

### Step 6 — Confirm the outputs
- **Gold tables** registered in Glue DB `ocbc_poc_dev_gold`: `customer_360`, `daily_txn_summary`, `account_balances`.
- **Audit trail** in `ocbc_poc_dev_audit` — one row per stage with status + row counts.
- **Lineage + quality JSON** under the data-lake bucket's `lineage/<run_id>/` and `quality/<run_id>/` prefixes.

### Step 7 — Query gold in Athena (`us-east-1`)
1. Select workgroup **`ocbc-poc-dev-wg`** (results location + encryption are enforced).
2. Point at database **`ocbc_poc_dev_gold`**.
3. Run the examples in `sql/athena/`:
   `01_gold_customer_360.sql` · `02_gold_daily_txn_summary.sql` · `03_gold_account_balances.sql` · `04_audit_run_history.sql`.

### Running a single stage directly on EMR (optional / debugging)
Normally Step Functions drives all four stages. To run just one stage as a standalone EMR Serverless job (e.g. to re-run gold), submit its script with the shared args:
```bash
aws emr-serverless start-job-run \
  --application-id "$(terraform output -raw emr_application_id)" \
  --execution-role-arn "$(terraform output -raw emr_job_role_arn)" \
  --region us-east-1 \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'"$(terraform output -raw datalake_bucket)"'/src/jobs/canonical_to_gold.py",
      "entryPointArguments": ["--run-id","manual-rerun","--env","dev","--all"]
    }
  }'
```
Swap the `entryPoint` filename for `landing_to_bronze.py`, `bronze_to_silver.py`, or `silver_to_canonical.py` to run a different stage. Stages are order-dependent (each reads the previous layer), so prefer the Step Functions path for a full run.

### Demonstrating the data-quality gate
To show the pipeline halting on bad data (by design), regenerate with an injected failure, re-upload, and start a new execution:
```bash
python -m data.generator.generate --out ./_local_out --inject-dq-failure
python scripts/upload_landing.py --src ./_local_out --env dev --account-id <YOUR_ACCOUNT_ID>
# start a new Step Functions execution — it will stop at BronzeToSilver → PipelineFailed
```
The failure reason is recovered from `quality/<run_id>/` JSON and the audit row with `status=DQ_FAILED`.

### Teardown (cost hygiene)
```bash
cd infra/terraform && terraform destroy   # empty S3 buckets first if destroy complains
```

---
*See `INSTALL.md` for full setup, `docs/architecture.md`, `docs/finops.md`, `docs/security.md`, `docs/lineage_quality_observability.md`, and `docs/pii_catalog.md` for depth.*
