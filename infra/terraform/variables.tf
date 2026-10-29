# ===========================================================================
# variables.tf -- all tunable inputs (design S9)
# ===========================================================================
# Deterministic naming is driven by `project` + `env` (design S2.1): every
# resource name derives from name_prefix = "${project}-${env}" = "ocbc-poc-dev".
# Lifecycle day counts and the EMR capacity cap are variables so a demo can
# retune FinOps behaviour without editing resource blocks. Sane POC defaults
# ship in terraform.tfvars.example.
# ===========================================================================

# --- Deterministic base (design S2.1) -------------------------------------
variable "project" {
  description = "Project slug; first half of name_prefix. Mirrors config/pipeline.yaml `project`."
  type        = string
  default     = "ocbc-poc"
}

variable "env" {
  description = "Environment slug; second half of name_prefix. Baseline is dev-only."
  type        = string
  default     = "dev"

  validation {
    # The baseline pipeline is dev-only (the job CLI only accepts --env dev).
    condition     = contains(["dev"], var.env)
    error_message = "This POC baseline only supports env = \"dev\"."
  }
}

variable "region" {
  description = "AWS region to deploy into. Mirrors config/pipeline.yaml `region`."
  type        = string
  default     = "ap-southeast-1"
}

# --- EMR Serverless capacity / auto-stop (FinOps, design S9) --------------
variable "idle_timeout_minutes" {
  description = "EMR Serverless auto-stop idle timeout (minutes). Keeps idle cost at zero."
  type        = number
  default     = 15
}

variable "max_cpu" {
  description = "EMR Serverless maximum_capacity cpu (cost ceiling)."
  type        = string
  default     = "8 vCPU"
}

variable "max_memory" {
  description = "EMR Serverless maximum_capacity memory (cost ceiling)."
  type        = string
  default     = "32 GB"
}

variable "max_disk" {
  description = "EMR Serverless maximum_capacity disk (cost ceiling)."
  type        = string
  default     = "200 GB"
}

# --- S3 lifecycle day counts (FinOps storage caps, design S9) -------------
variable "landing_expire_days" {
  description = "Expire landing/ objects after N days (transient raw)."
  type        = number
  default     = 30
}

variable "bronze_ia_days" {
  description = "Transition bronze/ to STANDARD_IA after N days."
  type        = number
  default     = 30
}

variable "bronze_expire_days" {
  description = "Expire bronze/ objects after N days."
  type        = number
  default     = 90
}

variable "lineage_quality_expire_days" {
  description = "Expire lineage/ and quality/ JSON after N days (transient telemetry)."
  type        = number
  default     = 90
}

variable "athena_results_expire_days" {
  description = "Expire athena-results objects after N days (query scratch)."
  type        = number
  default     = 14
}

variable "abort_multipart_days" {
  description = "Abort incomplete multipart uploads after N days (both buckets)."
  type        = number
  default     = 7
}

variable "log_retention_days" {
  description = "CloudWatch log group retention (both SFN + EMR groups)."
  type        = number
  default     = 14
}

# --- Optional tag overrides (FinOps cost allocation, design S9) -----------
# The six FinOps tags default in locals.tf; this map lets a caller override or
# extend them (e.g. a different CostCenter per demo) without editing code.
variable "tags" {
  description = "Extra/override tags merged onto the FinOps default_tags map."
  type        = map(string)
  default     = {}
}
