# ===========================================================================
# cloudwatch.tf -- two explicit, KMS-encrypted log groups (design S9)
# ===========================================================================
# Nothing is left implied: we create both log groups so retention + encryption
# are controlled, not defaulted to never-expire/unencrypted.
#   * sfn -- Step Functions execution history (vended logs). Wired into the
#     state machine logging_configuration (stepfunctions.tf).
#   * emr -- EMR Serverless driver/executor stdout, where the structured JSON
#     logs land. Wired into the startJobRun payload's monitoringConfiguration.
# Both use the data-lake CMK and a short 14-day retention (FinOps + security).
# ===========================================================================

resource "aws_cloudwatch_log_group" "sfn" {
  # The /aws/vendedlogs/states/ prefix is mandatory for SFN log delivery.
  name              = local.sfn_log_group_name
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.datalake.arn
}

resource "aws_cloudwatch_log_group" "emr" {
  name              = local.emr_log_group_name
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.datalake.arn
}
