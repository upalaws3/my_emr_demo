# Terraform module — OCBC POC medallion pipeline

This module provisions the AWS footprint for the baseline EMR Serverless
medallion pipeline: KMS CMK, two S3 buckets, six Glue databases, two IAM roles,
two CloudWatch log groups, the EMR Serverless application, the Step Functions
state machine (ASL rendered from `medallion.asl.json.tftpl`), and the Athena
workgroup.

> **This module is AUTHORED ONLY. No `terraform apply` is run in this task.**
> The authoring host has no `terraform` binary and no AWS access, so
> `init` / `validate` / `fmt` / `plan` / `apply` are all **skipped**.
> Correctness here is by inspection against design §8/§9. The commands below are
> the runbook for when you do have Terraform + AWS credentials.

## Files

| File | Purpose |
| --- | --- |
| `versions.tf` | Terraform >= 1.6, AWS provider ~> 5.x |
| `providers.tf` | AWS provider + FinOps `default_tags` |
| `variables.tf` | project/env/region, capacity, lifecycle day counts, tag overrides |
| `locals.tf` | deterministic names, account-id bucket names, Glue DB map, tag map |
| `kms.tf` | one rotating CMK + alias `alias/ocbc-poc-dev-datalake` |
| `s3.tf` | datalake + athena-results buckets (BPA, SSE-KMS, versioning, lifecycle) |
| `glue.tf` | six Glue catalog databases |
| `iam.tf` | EMR job role + SFN role (least-privilege; documented vended-logs exception) |
| `cloudwatch.tf` | SFN + EMR log groups (14-day retention, KMS) |
| `emr_serverless.tf` | SPARK application (auto-start/stop, capacity cap) |
| `stepfunctions.tf` | state machine + `templatefile` render + logging |
| `medallion.asl.json.tftpl` | the ASL definition (4 Task states + shared Fail + Succeed) |
| `athena.tf` | Athena v3 workgroup (SSE-KMS results, enforced config) |
| `outputs.tf` | bucket names, KMS arn/alias, EMR app id, SM arn, workgroup, Glue DBs |
| `terraform.tfvars.example` | sane POC defaults |

## Deploy runbook (requires Terraform + AWS credentials)

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # adjust if needed

terraform init
terraform fmt -check
terraform validate
terraform plan -out plan.tfplan
terraform apply plan.tfplan
```

After apply, sync the Spark job scripts to the location the ASL `entryPoint`
expects (the `jobs_s3_base` output), then upload landing data and start an
execution:

```bash
# Push the job code to s3://<datalake_bucket>/src/jobs/ (ASL entryPoint prefix)
aws s3 sync ../../src s3://$(terraform output -raw datalake_bucket)/src

# Generate + upload tiny landing data (from a host that HAS AWS creds)
python -m data.generator.generate --out ./_local_out
python scripts/upload_landing.py --src ./_local_out --env dev

# Kick off the pipeline
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw state_machine_arn)"
```

Query the gold tables through the workgroup (see `sql/athena/`):

```bash
aws athena start-query-execution \
  --work-group "$(terraform output -raw athena_workgroup)" \
  --query-string "file://../../sql/athena/01_gold_customer_360.sql"
```

## Teardown

```bash
cd infra/terraform
terraform destroy
```

> The S3 buckets have versioning enabled and the data-lake bucket holds the
> durable `audit/` prefix. `terraform destroy` will fail on non-empty buckets
> unless you empty them first — this is intentional friction so the durable
> audit record is not deleted by accident.
