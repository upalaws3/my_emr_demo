"""spark_session.py -- build a SparkSession wired for Iceberg + the Glue catalog.

Design S5.2. This is the ONLY module that holds Spark configuration, and even
here ``pyspark`` is imported INSIDE :func:`build_spark` so the module itself
byte-compiles and imports on a host without pyspark/Java.

On EMR Serverless (``local=False``) the session uses the Glue Data Catalog as
the Iceberg catalog. For the optional local smoke test (``local=True``) it swaps
to a Hadoop catalog pointed at a local ``warehouse/`` directory and skips Glue.
"""

from __future__ import annotations

# Iceberg runtime extension class (same for local + Glue).
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def build_spark(
    app_name: str,
    catalog: str = "glue_catalog",
    warehouse: str | None = None,
    local: bool = False,
):
    """Return a configured SparkSession.

    Parameters
    ----------
    app_name:
        Spark application name (shows up in the UI / logs).
    catalog:
        Iceberg catalog name (``glue_catalog`` in the baseline).
    warehouse:
        Iceberg warehouse root. On AWS pass the ``s3://.../warehouse/`` uri; for
        a local run pass a local directory (defaults to ``./warehouse`` there).
    local:
        When True, use a Hadoop catalog on local disk and skip Glue (smoke test).

    ``pyspark`` is imported here so the module stays import-safe without Spark.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    # Iceberg SQL extensions are required in both modes for MERGE/time-travel SQL.
    builder = builder.config("spark.sql.extensions", _ICEBERG_EXTENSIONS)

    if local:
        # Local Hadoop catalog on disk -- no AWS/Glue involved.
        local_warehouse = warehouse or "./warehouse"
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", local_warehouse)
        )
    else:
        # EMR Serverless: Iceberg backed by the Glue Data Catalog, data on S3.
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config(
                f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
        )
        if warehouse:
            builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)

    return builder.getOrCreate()
