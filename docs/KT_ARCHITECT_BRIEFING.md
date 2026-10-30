# OCBC EMR Medallion Pipeline — Architect KT & Demo Prep

*A fast, powerful briefing to walk into the customer demo confident. Region `us-east-1`.*

---

## 1. The 30-second pitch

A banking data pipeline that lands 4 source tables in S3 and refines them through a **medallion architecture** (landing → bronze → silver → canonical → gold) on **EMR Serverless (PySpark)**, with **data quality gates, lineage, an audit trail, and PII masking** built in. Provisioned by **Terraform**, orchestrated by **Step Functions**, queried by **Athena**. Built around four banking priorities: **FinOps, data security, lineage, tracking**.

---

## 2. Project structure — what is what, where is what

| Path | What lives here |
| --- | --- |
| `config/pipeline.yaml` | **Single source of truth.** Names, region, masking salt+catalog, DQ rules, reference data, gold-mart definitions. Edit here, not in code. |
| `src/common/` | Shared building blocks: `config_loader`, `io_paths` (all S3 paths/table names), `logging_util`, `masking`, `lineage`, `quality` (DQ + gate), `audit`, `spark_session` (Iceberg/Glue wiring). |
| `src/jobs/` | The 4 PySpark stage jobs + `_job_common.py` (shared CLI + Spark bootstrap). |
| `data/generator/` | Synthetic data generator + `schema_defs.py` (the 4 table schemas — the schema contract). |
| `scripts/` | `package_jobs.py` (build+upload the EMR `src.zip`), `upload_landing.py` (push sample data), `run_local_checks.py`. |
| `infra/terraform/` | All AWS resources + the Step Functions ASL template (`medallion.asl.json.tftpl`). |
| `sql/athena/` | 4 example gold/audit queries. |
| `docs/`, `README.md`, `INSTALL.md` | Architecture, FinOps, security, lineage, PII catalog, install + EMR-execution runbooks. |

**Key mental model:** config + schemas are declarative; `src/common` is the toolkit; `src/jobs` are the 4 stages; Terraform is the AWS footprint; Step Functions drives the run.

---

## 3. The flow — one bucket, prefixes per layer

There are **2 S3 buckets**: the data lake (all layers are *prefixes* inside it) and an Athena-results bucket. There is **no EMR cluster** — EMR Serverless spins workers on demand and auto-stops (the FinOps headline).

```
landing/   raw CSV+JSON (4 tables)        — schema-on-read
   |  landing_to_bronze.py                — type + reject-bad-rows -> Parquet
bronze/    typed Parquet
   |  bronze_to_silver.py                 — DATA QUALITY GATE -> Iceberg
silver/    cleansed + validated (Iceberg) — still UNMASKED
   |  silver_to_canonical.py              — PII MASKING -> Iceberg
canonical/ standardized + masked (Iceberg)
   |  canonical_to_gold.py                — business marts -> Iceberg
gold/      3 marts (Iceberg)              — Athena queries this
```

Each layer has its own Glue database (`ocbc_poc_dev_<layer>`). Cross-cutting prefixes: `lineage/`, `quality/`, `audit/`, `warehouse/` (Iceberg physical data).

**Why a canonical layer between silver and gold?** Silver = clean but raw-shaped and *unmasked*. Canonical = conformed + *masked*. Gold reads **only** canonical, so "PII never reaches gold unmasked" is true by construction — a strong security talking point.

---

## 4. The 4 tables (landing)

| Table | Format | Grain | PII columns |
| --- | --- | --- | --- |
| `customers` | CSV | one row / customer | first_name, last_name, email, phone, national_id, date_of_birth |
| `accounts` | CSV | one row / account (FK customer_id) | — |
| `cards` | CSV | one row / card (FK account_id) | card_number, cardholder_name |
| `transactions` | JSON | one row / transaction (FK account_id) | — |

Relationships: `customers 1—* accounts 1—* cards`, and `accounts 1—* transactions`. Mixed CSV/JSON is deliberate — it shows the pipeline conforming heterogeneous source formats (dimensions as flat extracts, transactions as event JSON).

---

## 5. Data quality gate (the "trust" story)

`bronze_to_silver` runs config-driven expectations (not-null, uniqueness, referential integrity, value ranges, allowed sets, row-count). Each rule has a **severity**:
- `critical` -> a failure **halts the whole pipeline** at silver (Step Functions routes to a single `PipelineFailed` state).
- `warn` -> recorded, pipeline continues.

Results are written as JSON to `quality/<run_id>/`. To **demo the gate firing on purpose**, regenerate data with `--inject-dq-failure` (injects a duplicate transaction_id), re-upload, re-run -> pipeline stops at silver with a `DQ_FAILED` audit row. Great "the guardrail works" moment.

---

## 6. PII masking (the "security" story) — in canonical

`silver_to_canonical` applies, per the config masking catalog:
- `national_id` -> **SHA-256(salt + value)** token (deterministic — same person, same token across runs)
- `card_number` -> last 4 only
- `email` -> partial mask
- `first_name` / `last_name` -> first character
- `phone` -> last 2 digits
- `date_of_birth` -> replaced by `birth_year` (int)

Salt precedence: `PIPELINE_MASK_SALT` env (set by Terraform) > `config.masking.salt` > hard fail. Masking is also recorded in **lineage** (`transform='mask:<method>'`), so you can prove *which* columns were masked and how.

---

## 7. Gold layer — the business value (lead with this)

Gold is built from **canonical** (already masked) as **3 marts**, each its own Iceberg table in `ocbc_poc_dev_gold`:

**1. `customer_360`** — one row per customer: a single pane combining account count, active-account count, card count, transaction count, and last-transaction time.
- *Key engineering point to mention:* it's built from **separate per-customer aggregations joined on customer_id**, NOT one flat join — a flat join of customers×accounts×cards×transactions fans out rows and corrupts the counts. This is the kind of correctness detail a bank's data team will respect.

**2. `daily_txn_summary`** — grain `account_id × txn_date × currency`: daily debit total, credit total, net amount, txn count. Money stays **per-currency** (no FX assumptions). This is the analytics/reporting mart.

**3. `account_balances`** — one row per account with its `customer_token` (the masked national_id), so analysts can group by customer without ever seeing PII.

**The business narrative:** raw, messy, multi-format banking data in -> a governed, masked, query-ready **Customer 360 + daily transaction analytics** out, in minutes, with full lineage and an audit trail. That's the value.

---

## 8. FinOps story (this customer cares)

- **EMR Serverless**: no idle clusters, pay only for vCPU-hours a job uses, **auto-stops after 15 min idle**. Show `billedResourceUtilization` on a job run.
- **Hard capacity ceiling** (32 vCPU/128 GB) caps runaway spend; small bounded executors keep usage minimal.
- **S3 lifecycle rules**: landing expires at 30d, bronze tiers to IA then expires, lineage/quality at 90d — storage doesn't grow unbounded. (`audit/` is intentionally exempt — durable record.)
- **Cost-allocation tags** on every resource (`CostCenter`, `Project`, `Environment`, `Owner`…) via Terraform `default_tags`.

---

## 9. Security story

- **SSE-KMS** (customer-managed key) on all S3; **TLS-only + account-only** bucket policies (no public access).
- **Least-privilege IAM**: separate scoped roles for the EMR job and for Step Functions.
- **PII masked at canonical**; gold consumers never see raw PII.
- **Private VPC**: the deploy ran through **VPC interface endpoints** (no public internet needed) — a genuine banking-grade posture.

---

## 10. How a run actually executes (so you can narrate live)

1. **Step Functions** `ocbc-poc-dev-medallion-pipeline` runs 4 stages in sequence, each an **EMR Serverless `startJobRun.sync`** task.
2. The **execution name = run_id**, threaded into every job (`--run-id`), every lineage/quality/audit record — one execution is fully traceable end to end.
3. Any stage failure -> single shared `PipelineFailed` state. Success -> `PipelineSucceeded`.
4. Jobs read their shared code + config from `src.zip` (shipped via `spark.submit.pyFiles`).

**Demo surfaces, in order of impact:** Step Functions graph (green stages) -> S3 buckets filling per layer -> Athena query on `customer_360` -> CloudWatch logs (observability) -> EMR Serverless job runs (billed utilization = FinOps).

---

## 11. Operational cheat-sheet

| Task | Command (from `infra/terraform`) |
| --- | --- |
| Get resource names/ARNs | `terraform output` |
| Rebuild+upload job code (after any `src/` change) | `python scripts/package_jobs.py --upload --account-id 656111643793` |
| Start a run | `aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" --name demo-$(date +%s) --region us-east-1` |
| Watch job runs | `aws emr-serverless list-job-runs --application-id <app-id> --region us-east-1 --query "jobRuns|sort_by(@,&createdAt)[-6:].[name,state]" --output table` |
| Teardown | `terraform destroy` (empty S3 buckets first) |

**Golden rule:** any change under `src/`, `data/`, or `config/` -> **rebuild+reupload the zip** before re-running. Infra changes (Terraform) -> `terraform apply`.

---

## 12. Likely customer questions — crisp answers

- *"Where's the cluster?"* -> EMR Serverless, no cluster — workers on demand, auto-stop, zero idle cost.
- *"Why a canonical layer?"* -> to guarantee gold never sees unmasked PII, and to conform data before business logic.
- *"How do you prove lineage?"* -> per-run JSON in `lineage/`, column-level mappings incl. masking transforms, registered in Glue.
- *"What happens on bad data?"* -> critical DQ expectation halts the pipeline at silver; `DQ_FAILED` audit row + quality JSON explain why.
- *"How is PII protected?"* -> SSE-KMS at rest, TLS in transit, masked/tokenized at canonical, least-privilege IAM, private VPC.
- *"Is this production-ready?"* -> it's a clean **baseline**; next iterations add OpenLineage/Marquez, heavier Great Expectations, Lake Formation column policies, and a cost dashboard.

---
*Deeper detail: `docs/architecture.md`, `docs/finops.md`, `docs/security.md`, `docs/lineage_quality_observability.md`, `docs/pii_catalog.md`, and `docs/OVERVIEW_AND_EMR_EXECUTION.md`.*
