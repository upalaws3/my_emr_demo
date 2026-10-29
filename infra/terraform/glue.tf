# ===========================================================================
# glue.tf -- six Glue catalog databases, one per layer + audit (design S2.3)
# ===========================================================================
# AC4 requires each layer to map to its own Glue database. The Spark jobs write
# managed Iceberg tables into silver/canonical/gold/audit and a Parquet table
# into bronze. `landing` is created too (for AC4 + optional crawler use) but the
# jobs write no managed table into it -- it holds raw CSV/JSON only (design
# S2.3). Names use underscores because Athena/Hive identifiers disallow hyphens.
#
# for_each over local.glue_databases keeps this to one resource block; the map
# values are the exact DB names (ocbc_poc_dev_<layer>).
# ===========================================================================

resource "aws_glue_catalog_database" "layers" {
  for_each = local.glue_databases

  name        = each.value
  description = "${local.name_prefix} ${each.key} layer (medallion). Managed by Terraform."
}
