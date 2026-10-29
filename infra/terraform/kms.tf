# ===========================================================================
# kms.tf -- one customer-managed CMK for the whole data lake (design S9)
# ===========================================================================
# A single CMK encrypts both S3 buckets, both CloudWatch log groups, and the
# Athena results. Key rotation is enabled. The key policy grants:
#   * the account root full admin (so the key is never orphaned), and
#   * the EMR job role + Step Functions role encrypt/decrypt (so Spark can
#     read/write SSE-KMS objects and SFN/CloudWatch can write encrypted logs).
# This is the "KMS CMK" pillar of the security story (see docs/security.md).
# ===========================================================================

resource "aws_kms_key" "datalake" {
  description             = "${local.name_prefix} data-lake CMK (S3 + CloudWatch Logs + Athena)"
  enable_key_rotation     = true  # annual automatic rotation (NFR: data security)
  deletion_window_in_days = 7     # short window is fine for a POC

  # Inline key policy. Written as jsonencode so it stays readable and valid.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Account root administers the key (prevents an unmanageable key).
        Sid       = "EnableRootAdmin"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        # The two pipeline roles may use the key for crypto operations only
        # (no admin). Scoped to encrypt/decrypt/generate-data-key actions.
        Sid    = "AllowPipelineRolesUseOfKey"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.emr_serverless_job_role.arn,
            aws_iam_role.step_functions_role.arn
          ]
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        # Allow the CloudWatch Logs service in this region to use the key so
        # the KMS-encrypted log groups (cloudwatch.tf) can be written.
        Sid    = "AllowCloudWatchLogs"
        Effect = "Allow"
        Principal = {
          Service = "logs.${var.region}.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            # Restrict to this account's log groups.
            "kms:EncryptionContext:aws:logs:arn" = "arn:aws:logs:${var.region}:${local.account_id}:log-group:*"
          }
        }
      }
    ]
  })
}

resource "aws_kms_alias" "datalake" {
  name          = local.kms_alias_name # alias/ocbc-poc-dev-datalake
  target_key_id = aws_kms_key.datalake.key_id
}
