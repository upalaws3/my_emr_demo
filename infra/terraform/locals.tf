# ===========================================================================
# locals.tf -- derived names + the FinOps tag map (design S2 / S9)
# ===========================================================================
# Naming is 100% deterministic from project + env (design S2.1), mirroring the
# logical names in config/pipeline.yaml (Terraform does NOT read the YAML; it
# re-derives the same names here). Bucket names carry the AWS account id so
# they are globally unique (design S2.2).
# ===========================================================================

# The account id is looked up (not hard-coded) so bucket names stay unique per
# account without manual edits.
data "aws_caller_identity" "current" {}

locals {
  # --- Deterministic base (design S2.1) -----------------------------------
  name_prefix = "${var.project}-${var.env}" # => ocbc-poc-dev
  account_id  = data.aws_caller_identity.current.account_id

  # --- S3 bucket names (design S2.2): base + account id -------------------
  datalake_bucket       = "${local.name_prefix}-datalake-${local.account_id}"
  athena_results_bucket = "${local.name_prefix}-athena-results-${local.account_id}"

  # --- Glue databases (design S2.3): underscores, one per layer + audit ---
  # Athena/Hive identifiers disallow hyphens, so the DB prefix uses underscores.
  db_prefix = replace(local.name_prefix, "-", "_") # => ocbc_poc_dev
  glue_databases = {
    landing   = "${local.db_prefix}_landing"
    bronze    = "${local.db_prefix}_bronze"
    silver    = "${local.db_prefix}_silver"
    canonical = "${local.db_prefix}_canonical"
    gold      = "${local.db_prefix}_gold"
    audit     = "${local.db_prefix}_audit"
  }

  # --- Named resources (design S2.4) --------------------------------------
  emr_application_name = "${local.name_prefix}-emr-serverless"      # ocbc-poc-dev-emr-serverless
  state_machine_name   = "${local.name_prefix}-medallion-pipeline"  # ocbc-poc-dev-medallion-pipeline
  athena_workgroup_name = "${local.name_prefix}-wg"                 # ocbc-poc-dev-wg
  kms_alias_name       = "alias/${local.name_prefix}-datalake"      # alias/ocbc-poc-dev-datalake

  # --- CloudWatch log group names (design S9) -----------------------------
  # The /aws/vendedlogs/states/ prefix is required by Step Functions log
  # delivery (vended-logs resource-policy path).
  sfn_log_group_name = "/aws/vendedlogs/states/${local.state_machine_name}"
  emr_log_group_name = "/aws/emr-serverless/${local.name_prefix}"

  # --- S3 job-script location (threaded into the ASL entryPoint) ----------
  # The four PySpark stage scripts live under src/jobs/ in the data-lake bucket
  # (the deploy runbook syncs the repo's src/ there). The ASL entryPoint is
  # s3://<bucket>/src/jobs/<script>.py (design S8). NO --job argument is passed;
  # each job derives its name from its own filename.
  jobs_s3_base = "s3://${local.datalake_bucket}/src/jobs"

  # --- FinOps tag map (design S9) -----------------------------------------
  # These six tags are applied to every taggable resource via the provider
  # default_tags block. `var.tags` can override/extend them per demo.
  common_tags = merge({
    Project            = var.project
    Environment        = var.env
    CostCenter         = "data-platform-poc"
    Owner              = "solutions-eng"
    ManagedBy          = "terraform"
    DataClassification = "confidential-pii"
  }, var.tags)
}
