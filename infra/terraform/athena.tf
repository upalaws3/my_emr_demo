# ===========================================================================
# athena.tf -- Athena v3 workgroup for querying the gold DB (design S9)
# ===========================================================================
# The gold Iceberg tables auto-register in Glue via the Spark jobs, so Athena
# v3 can query them with no manual CREATE TABLE (see sql/athena/00_setup_notes).
# The workgroup:
#   * pins results to the athena-results bucket with SSE_KMS,
#   * enforces its own config (users can't override the result location/crypto),
#   * publishes CloudWatch metrics (observability), and
#   * runs "Athena engine version 3" (Iceberg-native).
# No separate Athena database resource is needed -- the gold Glue DB is the
# schema.
# ===========================================================================

resource "aws_athena_workgroup" "wg" {
  name = local.athena_workgroup_name # ocbc-poc-dev-wg

  configuration {
    # Users cannot override the result location/encryption below (security).
    enforce_workgroup_configuration = true
    # Emit per-query metrics to CloudWatch (observability).
    publish_cloudwatch_metrics_enabled = true
    # Pin the Iceberg-native engine.
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.datalake.arn
      }
    }
  }
}
