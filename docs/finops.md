# FinOps

Cost control is a first-class requirement for the customer. The baseline is
designed so an idle pipeline costs effectively nothing and an active one has a
hard spending ceiling, with storage that ages itself out and spend that is fully
attributable in Cost Explorer.

## 1. Pay-per-use serverless + auto-stop (no idle cost)

The pipeline runs on **EMR Serverless**, billed per vCPU/GB-second only while a
job runs — there is no always-on cluster. The application also has
**auto-stop** enabled with a 15-minute idle timeout
(`idle_timeout_minutes = 15`), so after a run finishes the application stops and
bills nothing. **auto-start** spins workers up on submit, so nothing is
pre-warmed and billed between runs. This is the headline FinOps win: an idle
demo environment costs $0 of compute.

See `infra/terraform/emr_serverless.tf`.

## 2. Capacity ceiling (runaway-spend guard)

The application sets `maximum_capacity = { cpu = "8 vCPU", memory = "32 GB",
disk = "200 GB" }`. A misbehaving or accidentally huge job cannot scale past
this ceiling, so there is a hard upper bound on any single run's compute cost.
`initial_capacity` is omitted, so workers scale from zero (no pre-provisioned,
always-billed capacity).

## 3. Lifecycle expiry (storage growth cap)

S3 lifecycle rules (`infra/terraform/s3.tf`) age out transient data so storage
does not grow unbounded:

| Prefix | Rule |
| --- | --- |
| `landing/` | expire after 30 days (transient raw) |
| `bronze/` | transition to STANDARD_IA at 30 days, expire at 90 |
| `lineage/`, `quality/` | expire after 90 days (transient telemetry) |
| athena-results bucket | expire all objects after 14 days (query scratch) |
| both buckets | abort incomplete multipart uploads after 7 days |
| **`audit/`** | **NO expiry — durable tracking record (intentional)** |

Bucket keys are enabled on SSE-KMS encryption to reduce per-object KMS request
cost on high-object-count prefixes.

> The `audit/` prefix is deliberately exempt from expiry: it is the durable
> pipeline-run record (FR6/NFR4) and must never be aged out. All the other
> prefixes are transient and self-clean.

## 4. Cost-allocation tags (spend attribution)

The AWS provider sets `default_tags` so every taggable resource inherits a
consistent FinOps tag map (`infra/terraform/providers.tf` + `locals.tf`):

```
Project            = ocbc-poc
Environment        = dev
CostCenter         = data-platform-poc
Owner              = solutions-eng
ManagedBy          = terraform
DataClassification = confidential-pii
```

Once these cost-allocation tags are activated in the billing console, the
customer can slice spend in **Cost Explorer** by `CostCenter`, `Project`,
`Owner`, or `Environment` without per-resource tagging work. `var.tags` lets a
demo override/extend the map.

## 5. Athena query-cost control

Athena is billed per TB scanned. The `ocbc-poc-dev-wg` workgroup
(`infra/terraform/athena.tf`) has `enforce_workgroup_configuration = true` so
the result location and encryption cannot be overridden, and
`publish_cloudwatch_metrics_enabled = true` so bytes-scanned per query is
observable. Combined with the 14-day result-bucket expiry, query scratch never
accumulates cost. (A bytes-scanned cutoff per query can be added to the
workgroup as a later hardening step.)

## What is out of scope (future)

Cost dashboards, budget alerts/actions, Savings Plans, and per-query
bytes-scanned caps are deliberately **not** in this baseline. The tagging +
auto-stop + capacity-cap + lifecycle foundation is what makes those easy to add
later.
