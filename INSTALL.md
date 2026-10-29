# INSTALL — step-by-step setup (OCBC EMR medallion POC)

A plain, copy-pasteable guide to go from a clone to a running pipeline and
Athena queries. The demo region is **`us-east-1`** everywhere in this guide.

This is the install runbook. For the *why* behind each piece, cross-link to:
[`README.md`](README.md),
[`docs/architecture.md`](docs/architecture.md),
[`docs/finops.md`](docs/finops.md),
[`docs/security.md`](docs/security.md),
[`docs/lineage_quality_observability.md`](docs/lineage_quality_observability.md),
[`docs/pii_catalog.md`](docs/pii_catalog.md),
[`infra/terraform/README.md`](infra/terraform/README.md), and
[`sql/athena/00_setup_notes.md`](sql/athena/00_setup_notes.md).

---

## 1. Prerequisites

| Tool | Version | Needed for | Install link |
| --- | --- | --- | --- |
| Python | 3.11+ (EMR target is 3.11) | generator, tests, local checks, scripts | https://www.python.org/downloads/ |
| Terraform | ~> 1.5 (1.6+ fine) | provisioning AWS infra | https://developer.hashicorp.com/terraform/install |
| AWS CLI | v2 | credentials, starting the pipeline, syncing code | https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html |
| Java | 11 or 17 (**optional**) | ONLY the optional local Spark smoke test | https://adoptium.net/ |
| AWS account | — | deploy + run | https://aws.amazon.com/ |

AWS credentials: run `aws configure` (or export env vars) for an identity in
**`us-east-1`** with permissions to create/manage: S3, Glue, EMR Serverless,
Step Functions, Athena, KMS, IAM, and CloudWatch Logs. See
[`docs/security.md`](docs/security.md) for the least-privilege intent.

```bash
aws configure
# AWS Access Key ID / Secret / Default region name: us-east-1 / output: json
```

> The local authoring host does NOT need AWS, Terraform, or Java to run
> sections 3 below — only Python. Sections 5–8 require AWS + Terraform.

---

## 2. Get the code

```bash
git clone <your-repo-url> ocbc-poc
cd ocbc-poc
```

Repository layout (mirrors [`README.md`](README.md)):

| Path | What's there |
| --- | --- |
| `config/pipeline.yaml` | single source of truth: names, region, salt, reference data, DQ rules |
| `data/generator/` | stdlib synthetic data generator + schema defs |
| `scripts/` | landing upload, local-checks orchestrator, optional Spark smoke |
| `src/common/` | config, io_paths, logging, masking, lineage, quality, audit, spark |
| `src/jobs/` | the four PySpark stage jobs |
| `infra/terraform/` | Terraform module + Step Functions ASL template |
| `sql/athena/` | setup notes + four example queries |
| `docs/` | architecture, finops, security, lineage/quality/observability, PII catalog |
| `tests/` | pure-Python unit tests |
| `INSTALL.md` | this guide |

---

## 3. Local Python setup (NO AWS required)

Create a virtual environment and install the two runtime deps:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

Generate synthetic landing data:

```bash
python -m data.generator.generate --out ./_local_out
```

Expected output (counts vary slightly within the configured ranges):

```
generated landing data into ./_local_out:
  customers: 110 rows
  accounts: 143 rows
  cards: 110 rows
  transactions: 889 rows
```

This writes `customers.csv`, `accounts.csv`, `cards.csv`, and
`transactions.json` into `./_local_out`.

Run the unit tests:

```bash
python -m pytest tests/ -q
```

Expected tail:

```
44 passed in 0.3s
```

Run the one-command local gate (compile + import-safe + generator + pytest):

```bash
python scripts/run_local_checks.py
```

Expected summary (steps 5–6 SKIP on a host without Terraform/Java/pyspark):

```
==================== local checks summary ====================
  1. compileall + py_compile   PASS     compileall + per-job py_compile OK
  2. import-safe common        PASS     all src/common modules import-safe with public API present
  3. generator in-range + PII  PASS     generator counts in range + PII populated (...)
  4. pytest                    PASS     44 passed ...
  5. terraform                 SKIPPED  terraform not installed
  6. spark smoke               SKIPPED  pyspark and/or java not available
==============================================================
RESULT: OK (mandatory steps passed)
```

---

## 4. Configure the pipeline

Everything tunable lives in **`config/pipeline.yaml`** (the single source of
truth read via `src/common/config_loader.py`). Key knobs:

| Key | What it controls |
| --- | --- |
| `project` / `env` | deterministic name prefix (`ocbc-poc-dev`) for every resource |
| `region` | demo AWS region — set to **`us-east-1`** |
| `environments` | allowed `--env` values for the jobs (the POC ships `[dev]`) |
| `buckets.datalake` / `buckets.athena_results` | S3 base names (account id appended at deploy/runtime) |
| `masking.salt` | PII token salt — override at runtime with env var `PIPELINE_MASK_SALT` |
| `masking.columns` | which columns are masked and by which method, per table |
| `reference_data` | single source of truth for coded sets (countries, currencies, account/card/txn enums) |
| `gold_marts` | the gold mart names + each mart's lineage input tables |
| `dq_rules` | per-table data-quality expectations and their `critical` / `warn` severities |

The salt precedence is: env `PIPELINE_MASK_SALT` > `config.masking.salt` > hard
fail. For a demo:

```bash
# optional: override the salt without editing the file
export PIPELINE_MASK_SALT="my-demo-salt"      # macOS/Linux
$env:PIPELINE_MASK_SALT = "my-demo-salt"      # Windows PowerShell
```

Then set up the Terraform variables:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` — at minimum confirm:

```hcl
project = "ocbc-poc"
env     = "dev"
region  = "us-east-1"

# optional cost-allocation tags merged onto the FinOps default_tags
# tags = { CostCenter = "my-demo-cost-center" }
```

Your AWS account id is discovered by Terraform from your credentials (it is
appended to the globally-unique bucket names); you do not hardcode it here.

---

## 5. Deploy to AWS with Terraform

> This CANNOT run on the local authoring host used to build this POC
> (no Terraform, no AWS credentials). Run it from a workstation/CI that has
> both.

```bash
cd infra/terraform
terraform init
terraform plan      # review what will be created
terraform apply     # type "yes" to confirm
```

What gets created (`us-east-1`): a KMS CMK, the two S3 buckets, the six Glue
databases, an EMR Serverless application, the Step Functions state machine, the
Athena workgroup (`ocbc-poc-dev-wg`), and the IAM roles. See
[`infra/terraform/README.md`](infra/terraform/README.md) for the full inventory
and [`docs/finops.md`](docs/finops.md) for the cost controls.

---

## 6. Load data and run the pipeline

Sync the job code to the S3 location the Step Functions `entryPoint` expects,
then upload the generated landing files (run from a host WITH AWS creds):

```bash
cd infra/terraform

# 1. push the Spark job code to s3://<datalake_bucket>/src
aws s3 sync ../../src "s3://$(terraform output -raw datalake_bucket)/src" --region us-east-1

# 2. upload the generated landing files (account id is part of the bucket name)
python ../../scripts/upload_landing.py --src ../../_local_out --env dev \
  --account-id <YOUR_ACCOUNT_ID>
```

Start the pipeline — either from the Step Functions console (pick the
`ocbc-poc-dev-medallion-pipeline` state machine and choose **Start execution**),
or from the CLI using the ARN from `terraform output`:

```bash
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --region us-east-1
```

Watch it run:

- **Step Functions console** (`us-east-1`): the graph shows
  `LandingToBronze → BronzeToSilver → SilverToCanonical → CanonicalToGold`.
  A critical DQ failure routes to the shared `PipelineFailed` state.
- **CloudWatch Logs** (`us-east-1`): the Step Functions log group
  (`/aws/vendedlogs/states/ocbc-poc-dev-medallion-pipeline`) and the EMR log
  group (`/aws/emr-serverless/ocbc-poc-dev`). See
  [`docs/lineage_quality_observability.md`](docs/lineage_quality_observability.md).

---

## 7. Query results in Athena

In the Athena console (`us-east-1`):

1. Select the **`ocbc-poc-dev-wg`** workgroup (its results location + encryption
   are enforced — nothing to configure per query).
2. Point the editor at the **`ocbc_poc_dev_gold`** database.
3. Run the example queries in `sql/athena/`:
   - `01_gold_customer_360.sql`
   - `02_gold_daily_txn_summary.sql`
   - `03_gold_account_balances.sql`
   - `04_audit_run_history.sql` (against `ocbc_poc_dev_audit`)

The Spark jobs register every Iceberg table in Glue as they write, so there is
no manual `CREATE TABLE`. See
[`sql/athena/00_setup_notes.md`](sql/athena/00_setup_notes.md).

---

## 8. Teardown / cost hygiene

```bash
cd infra/terraform
terraform destroy   # type "yes" to confirm
```

If `destroy` fails because an S3 bucket is not empty, empty it first, then
re-run:

```bash
aws s3 rm "s3://$(terraform output -raw datalake_bucket)" --recursive --region us-east-1
aws s3 rm "s3://$(terraform output -raw athena_results_bucket)" --recursive --region us-east-1
terraform destroy
```

FinOps notes (details in [`docs/finops.md`](docs/finops.md)): EMR Serverless
auto-stops when idle (so idle cost trends to zero), S3 lifecycle rules expire
transient landing/bronze/telemetry objects, and every resource carries
cost-allocation tags via Terraform `default_tags`.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `terraform apply` fails: bucket name already taken | S3 bucket names are global | Change `buckets.*` base names in `config/pipeline.yaml` and the mirrored Terraform locals, then re-apply |
| `AccessDenied` on KMS/IAM during apply or run | deploying identity lacks permissions | Grant the IAM perms listed in section 1 (S3/Glue/EMR/SFN/Athena/KMS/IAM/CloudWatch) |
| Pipeline halts at `BronzeToSilver` with a DQ failure | a critical DQ expectation tripped — this is BY DESIGN when data is bad, and is how `--inject-dq-failure` demonstrates the gate | Regenerate clean data: `python -m data.generator.generate --out ./_local_out` (omit `--inject-dq-failure`), re-upload, re-run |
| Athena error: results location not set | querying outside the enforced workgroup | Select the `ocbc-poc-dev-wg` workgroup before running queries |
| Resources/queries target the wrong region | region mismatch | Confirm `region: us-east-1` in `config/pipeline.yaml`, `region = "us-east-1"` in `terraform.tfvars`, and `--region us-east-1` on CLI calls |

To intentionally demonstrate the DQ gate firing, regenerate with the injected
failure and run the pipeline:

```bash
python -m data.generator.generate --out ./_local_out --inject-dq-failure
```
