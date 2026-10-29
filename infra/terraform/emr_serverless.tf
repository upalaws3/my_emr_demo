# ===========================================================================
# emr_serverless.tf -- one SPARK EMR Serverless application (design S9)
# ===========================================================================
# FinOps-first configuration:
#   * auto_start  -> workers spin up on job submit (no always-on cost).
#   * auto_stop   -> idle application stops after idle_timeout_minutes, so an
#                    idle app costs nothing (the headline FinOps win).
#   * maximum_capacity -> a hard ceiling so a runaway job can't rack up spend.
#   * initial_capacity -> omitted so workers scale from zero (no pre-warmed,
#                         always-billed capacity for a POC).
#
# Log destination note (design S9): the job-run CloudWatch logging is NOT set
# on this application resource. EMR Serverless attaches monitoring at the
# job-run level, so it is configured on the Step Functions startJobRun payload
# (configurationOverrides.monitoringConfiguration -> the emr log group). See
# the ASL template medallion.asl.json.tftpl and stepfunctions.tf.
# ===========================================================================

resource "aws_emrserverless_application" "medallion" {
  name          = local.emr_application_name # ocbc-poc-dev-emr-serverless
  type          = "SPARK"
  release_label = "emr-7.5.0" # EMR 7.x ships Spark 3.5 (matches requirements.txt pin)

  # Scale workers from zero on submit (FinOps: no idle warm pool).
  auto_start_configuration {
    enabled = true
  }

  # Stop the application after it sits idle (FinOps: zero idle cost).
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  # Hard spend ceiling: a misbehaving job cannot scale past this.
  maximum_capacity {
    cpu    = var.max_cpu    # "8 vCPU"
    memory = var.max_memory # "32 GB"
    disk   = var.max_disk   # "200 GB"
  }

  # initial_capacity intentionally omitted -> no pre-provisioned (billed)
  # workers; the application scales from zero when a job run starts.
}
