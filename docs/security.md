# Security

Data security is a key customer priority. The baseline encrypts everything at
rest with a customer-managed key, blocks all public access to data, and scopes
IAM to least-privilege — with exactly one documented, unavoidable exception.

## 1. Encryption at rest — one customer-managed KMS key

A single customer-managed CMK (`alias/ocbc-poc-dev-datalake`,
`infra/terraform/kms.tf`) encrypts:

- both S3 buckets (SSE-KMS default encryption, `infra/terraform/s3.tf`),
- both CloudWatch log groups (`kms_key_id`, `infra/terraform/cloudwatch.tf`),
- Athena query results (`encryption_configuration` SSE_KMS,
  `infra/terraform/athena.tf`).

**Key rotation is enabled** (`enable_key_rotation = true`). The key policy grants
the account root admin (so the key is never orphaned) and grants the two
pipeline roles crypto-only use (`Encrypt`/`Decrypt`/`GenerateDataKey*`/
`ReEncrypt*`/`DescribeKey`), plus the CloudWatch Logs service in-region use
scoped by encryption context. No broad `kms:*` is granted to the pipeline roles.

## 2. Block Public Access on all buckets

Both buckets set all four `aws_s3_bucket_public_access_block` flags to `true`
(`block_public_acls`, `block_public_policy`, `ignore_public_acls`,
`restrict_public_buckets`). The data lake is never reachable publicly. The
data-lake bucket also has versioning enabled to protect Iceberg metadata and the
durable audit record from accidental overwrite/delete.

## 3. Least-privilege IAM

Two roles (`infra/terraform/iam.tf`), each scoped as tightly as the service
allows:

**EMR Serverless job role** — assumed only by `emr-serverless.amazonaws.com`
(trust conditioned on `aws:SourceAccount` and the specific application ARN via
`aws:SourceArn`). Its policy enumerates resources — **no `s3:*`, no `logs:*`,
no `kms:*`**:

- S3: `ListBucket`/`GetBucketLocation` on the two bucket ARNs; `GetObject`/
  `PutObject`/`DeleteObject` on their object space only.
- Glue: catalog + the six databases + their tables only.
- KMS: crypto actions on the one CMK only.
- Logs: `CreateLogStream`/`PutLogEvents` on the EMR log group ARN (+`:*`) only.

**Step Functions role** — assumed only by `states.amazonaws.com`. It can
`StartJobRun`/`GetJobRun`/`CancelJobRun` on the specific EMR application ARN,
`iam:PassRole` the job role (conditioned on `iam:PassedToService =
emr-serverless.amazonaws.com`), and perform the CloudWatch Logs delivery
actions for execution logging.

## 4. Documented least-privilege exception — SFN vended logs

The Step Functions `logging_configuration` requires a set of CloudWatch Logs
**log-delivery** actions, and AWS requires them to be granted with
`Resource = "*"` — they **do not support resource scoping**:

```
logs:CreateLogDelivery, logs:GetLogDelivery, logs:UpdateLogDelivery,
logs:DeleteLogDelivery, logs:ListLogDeliveries, logs:PutResourcePolicy,
logs:DescribeResourcePolicies, logs:DescribeLogGroups
```

This is the **one intentional deviation from least-privilege** in the module
(see the `SfnVendedLogsDelivery` statement in `iam.tf`). It is an AWS platform
requirement for vended-logs delivery, not a design choice, and it is confined to
the Step Functions role (which cannot touch data). Everything else in both roles
is resource-scoped.

## 5. PII handling

PII is masked at the canonical layer and never reaches gold or Athena in the
clear (see `pii_catalog.md`). `national_id` is tokenized (SHA-256 over a salted
value, first 16 hex chars) so it can still serve as a stable join key without
exposing the raw value. The `DataClassification = confidential-pii` tag marks
every resource.

## Out of scope (future hardening)

A KMS-backed tokenization vault or reversible tokenization, Lake Formation
fine-grained/row-column access control, VPC endpoints/private networking, and
automated key-policy/IAM drift detection are deliberately **not** in this
baseline. The CMK + BPA + least-privilege foundation is what they build on.
