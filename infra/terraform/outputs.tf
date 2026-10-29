# ===========================================================================
# outputs.tf -- everything the scripts / runbook / demo need (design S9)
# ===========================================================================

output "datalake_bucket" {
  description = "Data-lake bucket name (all medallion layers as prefixes)."
  value       = aws_s3_bucket.datalake.bucket
}

output "athena_results_bucket" {
  description = "Athena query-results bucket name."
  value       = aws_s3_bucket.athena_results.bucket
}

output "kms_key_arn" {
  description = "Data-lake CMK ARN."
  value       = aws_kms_key.datalake.arn
}

output "kms_key_alias" {
  description = "Data-lake CMK alias."
  value       = aws_kms_alias.datalake.name
}

output "emr_application_id" {
  description = "EMR Serverless application id (threaded into the Step Functions startJobRun)."
  value       = aws_emrserverless_application.medallion.id
}

output "emr_job_role_arn" {
  description = "EMR Serverless job execution role ARN."
  value       = aws_iam_role.emr_serverless_job_role.arn
}

output "state_machine_arn" {
  description = "Step Functions medallion pipeline state machine ARN."
  value       = aws_sfn_state_machine.medallion.arn
}

output "athena_workgroup" {
  description = "Athena workgroup name for querying the gold DB."
  value       = aws_athena_workgroup.wg.name
}

output "glue_databases" {
  description = "Map of layer -> Glue database name (landing/bronze/silver/canonical/gold/audit)."
  value       = { for k, db in aws_glue_catalog_database.layers : k => db.name }
}

output "jobs_s3_base" {
  description = "S3 base URI the Spark job scripts are synced to (ASL entryPoint prefix)."
  value       = local.jobs_s3_base
}
