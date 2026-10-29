# ===========================================================================
# providers.tf -- AWS provider + the FinOps default_tags map (design S9)
# ===========================================================================
# Every taggable resource created by this module inherits default_tags, so the
# banking customer can slice spend in Cost Explorer by Project / CostCenter /
# Owner without tagging each resource by hand. This is the FinOps "cost
# allocation tags" story (see docs/finops.md).
# ===========================================================================

provider "aws" {
  region = var.region

  # default_tags are merged onto every resource that supports tagging. The map
  # is assembled in locals.tf (locals.common_tags) so variables.tf can override
  # individual values without editing the provider block.
  default_tags {
    tags = local.common_tags
  }
}
