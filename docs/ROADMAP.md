# OCBC EMR Medallion Pipeline — Next-Iteration Roadmap

*What the baseline deliberately deferred, and where we take it next. Use this to answer "what's next?" with specifics. The baseline is a clean, working foundation; each item below is additive and independently shippable.*

---

## How to read this

The POC baseline proves the end-to-end shape: medallion + canonical on EMR Serverless, Iceberg, DQ gate, custom lineage, PII masking, audit, Athena — all Terraform + Step Functions. The items below deepen the four customer priorities (**FinOps, security, lineage, tracking**) or raise production-readiness. Effort is rough (S = days, M = 1–2 weeks, L = 3+ weeks) at POC scale.

---

## Tier 1 — Highest value for this customer

### 1. OpenLineage + Marquez (lineage) — **M**
**Today:** custom JSON lineage to S3 + Glue (lightweight, works, but bespoke).
**Next:** emit standard **OpenLineage** events from each Spark job and view them in **Marquez** (or a managed lineage catalog). Column-level lineage, a visual graph, and job/run/dataset facets out of the box.
**Why it matters:** a bank's governance team wants *standard, tool-agnostic* lineage they can audit and integrate — not a custom format. This is the single most credibility-adding upgrade.
**Touches:** add the OpenLineage Spark listener via `spark.extraListeners` conf; stand up Marquez (small ECS/EC2 or managed); keep the custom JSON as a fallback during transition.

### 2. Great Expectations (data quality) — **M**
**Today:** config-driven in-Spark assertions (not-null, unique, referential, ranges, sets, row-count) with a critical/warn gate.
**Next:** adopt the **Great Expectations** library for richer expectation types, auto-generated **Data Docs** (human-readable quality reports), and expectation suites per table versioned in the repo.
**Why it matters:** Data Docs give auditors a shareable quality report per run; the expectation catalog becomes a governance artifact. Keeps the existing gate semantics.
**Touches:** add GE as a packaged dependency (venv-pack for EMR Serverless, since it's a third-party lib); map current YAML rules to GE suites; write results to `quality/` and surface Data Docs in S3.

### 3. Lake Formation column/row-level security — **M/L**
**Today:** PII masked in canonical; gold consumers never see raw PII (structural).
**Next:** layer **AWS Lake Formation** for **column-level and row-level access policies** on the Glue tables — e.g. analysts see masked columns, a privileged role can see more, enforced at query time in Athena.
**Why it matters:** moves from "masked by pipeline design" to "access-controlled by policy" — the governance model a regulated bank expects. Enables need-to-know access without duplicating data.
**Touches:** register the data lake with Lake Formation; define permissions/tags (LF-Tags) per column; adjust IAM so Athena queries go through LF. Plan carefully — LF changes the catalog's permission model.

---

## Tier 2 — FinOps & observability depth

### 4. FinOps cost dashboard — **S/M**
**Today:** cost-allocation tags on every resource, EMR auto-stop, S3 lifecycle; `billedResourceUtilization` visible per job run.
**Next:** a **cost dashboard** — Cost & Usage Report (CUR) → Athena/QuickSight, or a CloudWatch dashboard — broken down by the existing cost-allocation tags (Project/Environment/CostCenter), plus per-run EMR cost from job-run utilization.
**Why it matters:** turns the FinOps *story* into a FinOps *control* the customer can watch. "Here's exactly what each pipeline run cost."
**Touches:** activate cost-allocation tags in Billing; CUR to S3; a small QuickSight dataset or CloudWatch dashboard. No pipeline code change.

### 5. Metrics & alerting on the audit table — **S**
**Today:** per-stage audit rows (status, row counts, duration) in S3 + an audit table.
**Next:** **CloudWatch metrics + alarms** derived from the audit/quality outputs — alert on pipeline failure, DQ gate firing, or row-count anomalies; optional SNS/email/Slack notification on `PipelineFailed`.
**Why it matters:** closes the observability loop — the team finds out a run failed without checking the console.
**Touches:** EventBridge rule on the Step Functions execution status → SNS; optional custom metrics from the audit writer.

---

## Tier 3 — Production-readiness

### 6. Orchestration & scheduling — **S**
Add an **EventBridge Scheduler** (or MWAA/Airflow if the customer standardizes on it) to run the pipeline on a cadence, and handle incremental loads rather than full refresh.

### 7. Incremental / CDC ingestion — **M/L**
Move landing from full-file drops to **incremental** (partitioned-by-date landing, Iceberg MERGE for upserts in silver/canonical). Demonstrates real-world ingestion, not just POC reloads.

### 8. CI/CD — **M**
A **GitHub Actions** (or CodePipeline) workflow: on merge to `main`, run tests, `terraform plan`, build + upload `src.zip`, and optionally `terraform apply` to a non-prod env. Removes the manual deploy steps from the runbook.

### 9. Multi-environment (dev/test/prod) — **S/M**
The baseline is `env=dev` only. Parameterize env across config + Terraform workspaces so the same code promotes through environments with isolated buckets/catalogs/roles.

### 10. Data contracts & schema registry — **M**
Formalize the landing schemas as versioned **data contracts** (today they live in `schema_defs.py`); reject or quarantine inputs that violate the contract, with contract versioning for source-system changes.

---

## Suggested sequencing

For *this* customer (governance-heavy bank): **OpenLineage/Marquez → Lake Formation → Great Expectations → cost dashboard**, because lineage + access control + auditable quality are the governance trifecta they'll weight most. Production-readiness (CI/CD, multi-env, incremental) follows once the governance story is locked.

Each item is independent — we can pick any one as the next sprint based on the customer's loudest need in the demo.

---
*Baseline reference: `docs/KT_ARCHITECT_BRIEFING.md` (§2 lists what's used vs. deferred).*
