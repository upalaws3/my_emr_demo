# ===========================================================================
# versions.tf -- Terraform + provider version pins (design S9)
# ===========================================================================
# POC note: this module is AUTHORED ONLY. It is NOT applied anywhere in this
# task -- `terraform init/validate/apply` are intentionally SKIPPED because
# terraform is absent from the authoring host. Correctness is by inspection
# against design S8/S9. See infra/terraform/README.md for the deploy runbook.
# ===========================================================================

terraform {
  # Terraform 1.6+ is required for the stable `moved`/`import` niceties and the
  # provider syntax used here. Pinned low so a reasonably recent CLI works.
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # AWS provider 5.x carries the EMR Serverless, Iceberg-aware Glue, and
      # Step Functions resources this module relies on.
      version = "~> 5.0"
    }
  }
}
