# ===========================================================================
# s3.tf -- the two buckets + Block Public Access + SSE-KMS + lifecycle (S9)
# ===========================================================================
# One data-lake bucket holds every medallion layer as a prefix; one separate
# bucket holds Athena query results (design S2.2). For BOTH buckets we enforce:
#   * Block Public Access (all four flags true)          -> security.md
#   * SSE-KMS with the CMK                                -> security.md
# plus lifecycle rules that cap storage growth (FinOps, finops.md).
#
# IMPORTANT (design S9): the `audit/` prefix is INTENTIONALLY exempt from any
# expiry rule -- it is the durable pipeline-run tracking record (FR6/NFR4) and
# must never be aged out. This omission is deliberate, not an oversight.
# ===========================================================================

# ---------------------------------------------------------------------------
# Data-lake bucket (landing / bronze / silver / canonical / gold / lineage /
# quality / audit / warehouse prefixes)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake_bucket
}

# Block Public Access: all four flags true (never public, design S9/security).
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Default encryption: SSE-KMS using the data-lake CMK, with bucket keys on to
# cut KMS request cost (a small FinOps win on high-object-count prefixes).
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.datalake.arn
    }
    bucket_key_enabled = true
  }
}

# Versioning on the data lake protects against accidental overwrite/delete of
# Iceberg metadata/data and the durable audit record.
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle: per-prefix expiry/transition (design S9). audit/ has NO rule.
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  # landing/ -> transient raw drops; expire after 30d.
  rule {
    id     = "landing-expire"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    expiration {
      days = var.landing_expire_days
    }
  }

  # bronze/ -> cool to STANDARD_IA after 30d, expire after 90d.
  rule {
    id     = "bronze-tier-and-expire"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    transition {
      days          = var.bronze_ia_days
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = var.bronze_expire_days
    }
  }

  # lineage/ -> transient telemetry; expire after 90d.
  rule {
    id     = "lineage-expire"
    status = "Enabled"
    filter {
      prefix = "lineage/"
    }
    expiration {
      days = var.lineage_quality_expire_days
    }
  }

  # quality/ -> transient telemetry; expire after 90d.
  rule {
    id     = "quality-expire"
    status = "Enabled"
    filter {
      prefix = "quality/"
    }
    expiration {
      days = var.lineage_quality_expire_days
    }
  }

  # ----------------------------------------------------------------------
  # audit/ is INTENTIONALLY NOT given an expiry rule. It is the durable
  # pipeline-run record and must survive indefinitely (design S9). Do not
  # add a rule for it. (silver/canonical/gold Iceberg data under warehouse/
  # is likewise left unmanaged by lifecycle -- table retention is an Iceberg
  # concern, not an S3 lifecycle one, in this POC.)
  # ----------------------------------------------------------------------

  # Clean up incomplete multipart uploads everywhere after 7d (cost hygiene).
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {} # applies to the whole bucket
    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_days
    }
  }
}

# ---------------------------------------------------------------------------
# Athena results bucket (query scratch; kept separate from the data lake)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket = local.athena_results_bucket
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.datalake.arn
    }
    bucket_key_enabled = true
  }
}

# Lifecycle: results are scratch; expire everything after 14d + abort stale
# multipart uploads after 7d.
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "athena-results-expire"
    status = "Enabled"
    filter {} # all objects in the results bucket
    expiration {
      days = var.athena_results_expire_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_days
    }
  }
}
