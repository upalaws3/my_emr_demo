# ===========================================================================
# stepfunctions.tf -- the medallion state machine (design S8 / S9)
# ===========================================================================
# The ASL definition lives in medallion.asl.json.tftpl and is rendered by
# templatefile() with the four interpolated values the ASL needs:
#   * emr_application_id -- which EMR Serverless app runs the jobs,
#   * job_role_arn       -- the execution role EMR passes to each job run,
#   * jobs_s3_base       -- s3://<bucket>/src/jobs (entryPoint prefix),
#   * emr_log_group_name -- destination for job-run CloudWatch logs.
# Execution logging goes to the vended-logs SFN log group (cloudwatch.tf).
# ===========================================================================

resource "aws_sfn_state_machine" "medallion" {
  name     = local.state_machine_name
  role_arn = aws_iam_role.step_functions_role.arn

  definition = templatefile("${path.module}/medallion.asl.json.tftpl", {
    emr_application_id = aws_emrserverless_application.medallion.id
    job_role_arn       = aws_iam_role.emr_serverless_job_role.arn
    jobs_s3_base       = local.jobs_s3_base
    jobs_pyfiles       = local.jobs_pyfiles
    emr_log_group_name = aws_cloudwatch_log_group.emr.name
  })

  # Full execution logging to the KMS-encrypted vended-logs group. The ":*"
  # suffix on the ARN is required by the Step Functions log-destination format.
  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  }
}
