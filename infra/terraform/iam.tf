# ===========================================================================
# iam.tf -- least-privilege roles for EMR Serverless + Step Functions (S9)
# ===========================================================================
# Two roles, each scoped as tightly as the service allows (security.md):
#   * emr_serverless_job_role -- assumed by EMR Serverless to run the Spark
#     jobs. Enumerated S3/Glue/KMS/Logs permissions. NO s3:*, NO logs:*.
#   * step_functions_role -- assumed by Step Functions to drive EMR job runs
#     and deliver vended logs. The CloudWatch Logs delivery actions MUST be
#     Resource="*" -- this is a DOCUMENTED exception to least-privilege (the
#     vended-logs requirement), called out in security.md.
# ===========================================================================

# ---------------------------------------------------------------------------
# EMR Serverless job role
# ---------------------------------------------------------------------------
# Trust: only the EMR Serverless service may assume it. Conditioned on the
# specific application ARN via aws:SourceArn (defence-in-depth against the
# confused-deputy problem).
data "aws_iam_policy_document" "emr_job_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_emrserverless_application.medallion.arn]
    }
  }
}

resource "aws_iam_role" "emr_serverless_job_role" {
  name               = "${local.name_prefix}-emr-job-role"
  assume_role_policy = data.aws_iam_policy_document.emr_job_assume.json
  description        = "EMR Serverless job execution role for the medallion pipeline (least-privilege)."
}

# Enumerated permissions -- no wildcards on service actions.
data "aws_iam_policy_document" "emr_job_policy" {
  # --- S3: the two buckets + their object space only --------------------
  statement {
    sid    = "S3BucketLevel"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.datalake.arn,
      aws_s3_bucket.athena_results.arn
    ]
  }
  statement {
    sid    = "S3ObjectLevel"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.datalake.arn}/*",
      "${aws_s3_bucket.athena_results.arn}/*"
    ]
  }

  # --- Glue: the catalog, the six DBs, and their tables -----------------
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:UpdatePartition"
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account_id}:catalog"],
      [for db in values(local.glue_databases) : "arn:aws:glue:${var.region}:${local.account_id}:database/${db}"],
      [for db in values(local.glue_databases) : "arn:aws:glue:${var.region}:${local.account_id}:table/${db}/*"]
    )
  }

  # --- KMS: crypto on the data-lake CMK only ----------------------------
  statement {
    sid    = "KmsUse"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]
    resources = [aws_kms_key.datalake.arn]
  }

  # --- CloudWatch Logs: write to the EMR log group only -----------------
  # CreateLogStream + PutLogEvents, scoped to the emr log group ARN (+:*).
  statement {
    sid    = "EmrLogsWrite"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:CreateLogGroup",
      "logs:DescribeLogStreams"
    ]
    resources = [
      aws_cloudwatch_log_group.emr.arn,
      "${aws_cloudwatch_log_group.emr.arn}:*"
    ]
  }

  # --- CloudWatch Logs: DescribeLogGroups (MUST be Resource="*") ---------
  # EMR Serverless calls logs:DescribeLogGroups when initializing a job run's
  # CloudWatch logging. This action does NOT support resource-level scoping, so
  # AWS requires Resource="*". Without it the job fails with
  # "Unable to push logs ... not authorized to perform: logs:DescribeLogGroups".
  # Split into its own statement so the write actions above stay scoped.
  statement {
    sid       = "EmrLogsDescribe"
    effect    = "Allow"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_job_policy" {
  name   = "${local.name_prefix}-emr-job-policy"
  role   = aws_iam_role.emr_serverless_job_role.id
  policy = data.aws_iam_policy_document.emr_job_policy.json
}

# ---------------------------------------------------------------------------
# Step Functions role
# ---------------------------------------------------------------------------
# Trust: only the Step Functions service may assume it.
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "step_functions_role" {
  name               = "${local.name_prefix}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  description        = "Step Functions execution role for the medallion pipeline (least-privilege + documented vended-logs exception)."
}

data "aws_iam_policy_document" "sfn_policy" {
  # --- EMR Serverless: drive job runs on the specific app only ----------
  statement {
    sid    = "EmrServerlessJobControl"
    effect = "Allow"
    actions = [
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:CancelJobRun"
    ]
    resources = [
      aws_emrserverless_application.medallion.arn,
      "${aws_emrserverless_application.medallion.arn}/jobruns/*"
    ]
  }

  # --- iam:PassRole: hand the EMR job role to EMR Serverless only --------
  statement {
    sid       = "PassEmrJobRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_serverless_job_role.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  # --- EventBridge managed rule for the .sync (startJobRun.sync) pattern --
  # The `arn:aws:states:::emr-serverless:startJobRun.sync` integration makes
  # Step Functions wait for the EMR job to finish. To do that, Step Functions
  # creates and manages an EventBridge rule that notifies it on job-state
  # changes. Without these actions, CreateStateMachine fails with
  # "is not authorized to create managed-rule". AWS fixes the managed rule's
  # name for the EMR Serverless integration, so we scope to that exact rule
  # ARN rather than "*". (Required by every .sync service integration.)
  statement {
    sid    = "SfnManagedRuleForEmrSync"
    effect = "Allow"
    actions = [
      "events:PutRule",
      "events:PutTargets",
      "events:DescribeRule",
    ]
    resources = [
      "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRule",
    ]
  }

  # --- CloudWatch Logs vended-logs delivery (DOCUMENTED Resource=* exception)
  # Step Functions logging_configuration requires these log-delivery actions,
  # and AWS requires them to be Resource="*" -- they do not support resource
  # scoping. This is the ONE intentional deviation from least-privilege in this
  # module and is called out in docs/security.md. (See design S9 IAM notes.)
  statement {
    sid    = "SfnVendedLogsDelivery"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]
    resources = ["*"] # REQUIRED by the SFN vended-logs contract; see security.md.
  }
}

resource "aws_iam_role_policy" "sfn_policy" {
  name   = "${local.name_prefix}-sfn-policy"
  role   = aws_iam_role.step_functions_role.id
  policy = data.aws_iam_policy_document.sfn_policy.json
}
