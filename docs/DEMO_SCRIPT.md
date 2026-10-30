# Live Demo Script — OCBC EMR Medallion Pipeline

*Click-by-click sequence with narration. ~15 min. Region `us-east-1`. Keep the KT briefing (`docs/KT_ARCHITECT_BRIEFING.md`) open in another tab.*

---

## Before the customer joins (5 min prep)

1. Confirm infra is up and get names:
   ```bash
   cd ~/my_emr_demo/infra/terraform && terraform output
   ```
2. Make sure a **successful run exists** (so you have green stages + gold data to show even if a live run is slow). If not, do one:
   ```bash
   python ../../scripts/package_jobs.py --upload --account-id 656111643793   # from repo root venv
   aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" --name prep-$(date +%s) --region us-east-1
   ```
3. Open these browser tabs, all in **us-east-1**: Step Functions, S3, Athena, CloudWatch Logs, EMR Serverless (via EMR Studio).
4. Have `docs/architecture.md` handy for the whiteboard moment.

---

## Act 1 — The story (2 min, no screen)

> "This is a banking data pipeline on AWS. Raw data from four source systems lands in S3; we refine it through a medallion architecture — bronze, silver, a canonical layer, then gold — running on EMR Serverless. Along the way we enforce data quality, mask PII, and capture full lineage and an audit trail. It's provisioned entirely in Terraform and orchestrated by Step Functions. Four things we optimized for, because they're what matters to a bank: cost, security, lineage, and tracking."

Show the medallion diagram (architecture.md). Point out the **canonical layer** — "that's where we mask PII, so the business/gold layer can never see raw personal data."

---

## Act 2 — Kick off a live run (1 min)

> "Let me start the pipeline. One command triggers the whole thing."

```bash
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw state_machine_arn)" \
  --name "live-demo-$(date +%Y%m%d-%H%M%S)" --region us-east-1
```

> "The execution name becomes the run-id, which flows into every job and every lineage, quality, and audit record — so a single run is traceable end to end."

Switch to the **Step Functions console** and open the execution. Let it run in the background; narrate over the already-completed prep run.

---

## Act 3 — Orchestration (2 min) — Step Functions

Open the prep run's **graph view**.

> "Each box is a stage running as an EMR Serverless Spark job. Landing to bronze types the raw data. Bronze to silver runs our data-quality gate. Silver to canonical masks PII. Canonical to gold builds the business marts. If any stage fails — including a critical data-quality failure — it routes to this single failure state and stops. No bad data reaches gold."

Point at the green path. Mention the `PipelineFailed` branch.

---

## Act 4 — Compute & FinOps (2 min) — EMR Serverless

EMR console -> EMR Serverless -> application -> **Job runs**.

> "Notice there's no cluster. This is EMR Serverless — workers spin up per job and the application auto-stops after 15 minutes idle, so idle cost is zero. We only pay for the exact vCPU-seconds each job used."

Open one job run -> show state, duration, and `billedResourceUtilization`.

> "Every resource is tagged for cost allocation, and S3 lifecycle rules keep storage from growing unbounded. That's the FinOps story."

---

## Act 5 — The data, layer by layer (2 min) — S3

Open the data-lake bucket. Walk the prefixes: `landing/` -> `bronze/` -> `silver/` -> `canonical/` -> `gold/`.

> "Same bucket, one prefix per layer. Landing is raw CSV and JSON. Bronze is typed Parquet. Silver, canonical, and gold are Apache Iceberg tables — so we get ACID, schema evolution, and time travel. And here —" open `lineage/` and `quality/` — "is the lineage JSON and the data-quality results for every run."

---

## Act 6 — Security & PII (1 min)

Open a `canonical/` or query it in Athena to show masked values.

> "This is the canonical layer. Card numbers are last-4 only, national IDs are tokenized with a salted hash, emails are partially masked. The gold layer reads only from here — so analysts get a full Customer 360 without ever touching raw PII. Everything's encrypted with a KMS key, TLS-only, and the whole thing deploys inside a private VPC with no public internet."

---

## Act 7 — Business value in Athena (3 min) — the payoff

Athena -> workgroup `ocbc-poc-dev-wg` -> database `ocbc_poc_dev_gold`. Run, in order:

1. `sql/athena/01_gold_customer_360.sql`
   > "A single row per customer — account count, card count, transaction count, last activity. The Customer 360 a bank wants, built from masked data."
2. `sql/athena/02_gold_daily_txn_summary.sql`
   > "Daily debit, credit, and net per account and currency — the analytics mart, with no cross-currency assumptions."
3. `sql/athena/04_audit_run_history.sql`
   > "And here's the audit trail — every stage of every run, with row counts and status. That's your tracking and observability surface."

> "So: raw, multi-format banking data in; governed, masked, query-ready analytics out — in minutes, fully tracked."

---

## Act 8 — The guardrail (optional, 2 min) — prove DQ works

> "Let me show you what happens when bad data arrives."

```bash
cd ~/my_emr_demo && source .venv/bin/activate
python -m data.generator.generate --out ./_local_out --inject-dq-failure
python scripts/upload_landing.py --src ./_local_out --env dev --account-id 656111643793
cd infra/terraform
aws stepfunctions start-execution --state-machine-arn "$(terraform output -raw state_machine_arn)" --name "dq-demo-$(date +%s)" --region us-east-1
```

In Step Functions, show it stop at **BronzeToSilver -> PipelineFailed**. Then show the `DQ_FAILED` audit row / `quality/` JSON.

> "The pipeline refused to promote bad data and recorded exactly why. That's the trust guarantee."

(Afterwards, regenerate clean data and re-upload so the environment is back to green.)

---

## Act 9 — Close (1 min)

> "Everything you saw is Terraform-provisioned and reproducible, and this is a baseline we extend. Natural next steps: richer lineage with OpenLineage and Marquez, deeper data-quality with Great Expectations, column-level access policies with Lake Formation, and a cost dashboard. Where would you want to go first?"

---

## Safety net — if a live run stalls

- You prepped a successful run — demo that one; don't wait on the live run.
- EMR cold start is 1–2 min on the first stage; fill with Act 1/architecture.
- If a stage errors live, pivot to the prep run's green graph and the Athena results. Don't debug on screen.
- Keep this handy to check a run:
  ```bash
  aws emr-serverless list-job-runs --application-id <app-id> --region us-east-1 \
    --query "jobRuns|sort_by(@,&createdAt)[-6:].[name,state]" --output table
  ```
