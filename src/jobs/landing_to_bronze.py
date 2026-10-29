"""landing_to_bronze.py -- land raw CSV/JSON as TYPED Parquet (design S6.1).

Stage 1 of the medallion flow. Responsibilities:
  * Read each landing file with an EXPLICIT schema derived from ``schema_defs``
    (schema-on-read). Everything is read as string first, then cast to the
    target bronze type so a bad value yields ``null`` (Spark cast behavior)
    rather than aborting the read.
  * Implement the BRONZE REJECT CONTRACT (design S6.1 Finding 2): a non-empty /
    non-null source value that casts to ``null`` is a reject. Rejected rows are
    written to ``bronze/<table>/_rejected/`` as Parquet carrying ALL original
    string columns plus a ``_reject_reason`` string (``<col>:uncastable``,
    semicolon-joined for multiple failures). ``input_rows = output_rows +
    rejected_rows``.
  * Add audit columns ``_ingested_at`` / ``_run_id`` / ``_source_file``.
  * Write the clean rows to ``bronze/<table>/`` as Parquet and register the
    table in Glue with the ordered ``(name, glue_type)`` pairs so Athena can
    read it (fmt='parquet' REQUIRES columns).
  * Emit per-table lineage (the LANDING input has NO glue_table key) and write
    ONE SUCCESS audit row per table. There is NO DQ gate here.

Spark/boto3 imports are LAZY so the module byte-compiles on the bare host.
"""

from __future__ import annotations

import pathlib
import sys
import time

# Support BOTH invocation styles: as a module (``python -m src.jobs.landing...``)
# and as a bare script (EMR Serverless sets entryPoint to the .py path, and
# ``python src/jobs/landing_to_bronze.py``). When run as a script there is no
# parent package, so we add the repo root to sys.path and import absolutely.
if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    from src.jobs import _job_common
else:
    from . import _job_common


def _landing_file_uri(ctx, table: str) -> str:
    """Return the landing file URI for a table.

    customers/accounts/cards are a single ``<table>.csv`` and transactions is
    ``transactions.json`` under the landing prefix (design S3).
    """
    from data.generator.schema_defs import TABLE_FORMATS
    from src.common import io_paths

    prefix = io_paths.s3_layer_prefix("landing", table, ctx)
    ext = "json" if TABLE_FORMATS[table] == "json" else "csv"
    return f"{prefix}{table}.{ext}"


def process_table(spark, cfg, ctx, run_id: str, table: str, local: bool) -> None:
    """Read one landing table, apply the reject contract, write bronze Parquet."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    from data.generator.schema_defs import TABLE_FORMATS, columns_for, glue_columns
    from src.common import io_paths, lineage, logging_util
    from src.common.audit import STATUS_SUCCESS, AuditRecord

    job = "landing_to_bronze"
    logger = logging_util.get_logger(run_id, "bronze", table)
    started_at = lineage._now_iso()
    start_monotonic = time.monotonic()

    cols = columns_for(table)
    col_names = [c.name for c in cols]
    source_uri = _landing_file_uri(ctx, table)

    logging_util.log_event(logger, logging_util.EVENT_JOB_START, table=table, source=source_uri)

    # --- read as ALL-STRING so we can detect cast failures explicitly --------
    string_schema = StructType([StructField(c.name, StringType(), True) for c in cols])
    if TABLE_FORMATS[table] == "json":
        raw = spark.read.schema(string_schema).json(source_uri)
    else:
        raw = spark.read.option("header", True).schema(string_schema).csv(source_uri)

    input_rows = raw.count()
    logging_util.log_event(logger, logging_util.EVENT_READ_DONE, row_count=input_rows)

    # --- cast each column + build a per-column reject reason ------------------
    # A reject = source value is non-empty/non-null but its cast is null.
    typed = raw
    reason_cols = []
    for c in cols:
        typed = typed.withColumn(f"__cast_{c.name}", F.col(c.name).cast(c.glue_type))
        # Uncastable iff original is present (not null, not blank) AND cast null.
        uncastable = (
            F.col(c.name).isNotNull()
            & (F.trim(F.col(c.name)) != F.lit(""))
            & F.col(f"__cast_{c.name}").isNull()
        )
        typed = typed.withColumn(
            f"__reason_{c.name}",
            F.when(uncastable, F.lit(f"{c.name}:uncastable")).otherwise(F.lit(None)),
        )
        reason_cols.append(F.col(f"__reason_{c.name}"))

    # Join the per-column reasons with ';' (nulls dropped by concat_ws).
    typed = typed.withColumn("_reject_reason", F.concat_ws(";", *reason_cols))

    rejected = typed.filter(F.col("_reject_reason") != F.lit(""))
    clean = typed.filter(F.col("_reject_reason") == F.lit(""))

    rejected_rows = rejected.count()
    output_rows = clean.count()

    # --- write rejects (all original string columns + _reject_reason) ---------
    reject_prefix = io_paths.s3_layer_prefix("bronze", table, ctx) + "_rejected/"
    if rejected_rows > 0:
        reject_out = rejected.select(*col_names, "_reject_reason")
        reject_out.write.mode("overwrite").parquet(reject_prefix)
        logging_util.log_event(
            logger, "reject_written", row_count=rejected_rows, path=reject_prefix
        )

    # --- build the clean typed DataFrame + audit columns ----------------------
    typed_select = [F.col(f"__cast_{c.name}").alias(c.name) for c in cols]
    bronze = clean.select(*typed_select)
    bronze = (
        bronze.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_file", F.lit(source_uri))
    )

    bronze_prefix = io_paths.s3_layer_prefix("bronze", table, ctx)
    with logging_util.timed(logger, "bronze_write", table=table):
        bronze.write.mode("overwrite").parquet(bronze_prefix)
    logging_util.log_event(logger, logging_util.EVENT_WRITE_DONE, row_count=output_rows)

    # --- register the Parquet table in Glue (columns REQUIRED for parquet) ----
    # Best-effort (design S10): a Glue register hiccup must not fail the data
    # path, so we log and continue.
    try:
        import boto3

        glue = boto3.client("glue")
        lineage.register_glue_table(
            glue,
            io_paths.glue_db("bronze", ctx),
            table,
            bronze_prefix,
            fmt="parquet",
            columns=glue_columns(table),
        )
    except Exception as exc:  # noqa: BLE001 - observability side-effect, best effort
        logging_util.log_event(logger, "glue_register_fail", message=str(exc))

    # --- lineage (landing input has NO glue_table key -- design Finding 4) ----
    builder = lineage.start_lineage(run_id, job, "landing", "bronze", table)
    builder.add_input(source_uri, TABLE_FORMATS[table], row_count=input_rows)  # no glue_table
    builder.add_output(
        bronze_prefix,
        "parquet",
        row_count=output_rows,
        glue_table=f"{io_paths.glue_db('bronze', ctx)}.{table}",
    )
    for name in col_names:
        builder.add_column_mapping(name, [name], "cast")
    builder.set_metrics(input_rows=input_rows, output_rows=output_rows, rejected_rows=rejected_rows)
    lineage_path = _emit_lineage(builder, local, ctx)

    # --- one SUCCESS audit row per table --------------------------------------
    ended_at = lineage._now_iso()
    duration_ms = int((time.monotonic() - start_monotonic) * 1000)
    record = AuditRecord(
        run_id=run_id,
        job=job,
        layer_from="landing",
        layer_to="bronze",
        table_name=table,
        status=STATUS_SUCCESS,
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=rejected_rows,
        dq_passed=True,  # no DQ gate at bronze
        dq_critical_failed=0,
        started_at=started_at,
        ended_at=ended_at,
        duration_ms=duration_ms,
        lineage_path=lineage_path,
    )
    _safe_write_audit(spark, record, ctx, logger)
    logging_util.log_event(logger, logging_util.EVENT_JOB_END, table=table, status=STATUS_SUCCESS)


def _emit_lineage(builder, local: bool, ctx) -> str | None:
    """Emit lineage; best-effort (design S10). Returns the path or None."""
    from src.common import lineage, logging_util

    logger = logging_util.get_logger(builder.run_id, builder.layer_to, builder.table)
    try:
        if local:
            return lineage.emit(builder, local_dir="./_lineage_out", ctx=ctx)
        import boto3

        return lineage.emit(builder, s3_client=boto3.client("s3"), ctx=ctx)
    except Exception as exc:  # noqa: BLE001 - observability side-effect
        logging_util.log_event(logger, "lineage_emit_fail", message=str(exc))
        return None


def _safe_write_audit(spark, record, ctx, logger) -> None:
    """Write the audit row; best-effort per design S10 (audit is observability)."""
    from src.common import io_paths, logging_util
    from src.common.audit import write_audit

    try:
        write_audit(spark, record, ctx.catalog, io_paths.glue_db("audit", ctx), ctx=ctx)
    except Exception as exc:  # noqa: BLE001
        logging_util.log_event(logger, "audit_write_fail", message=str(exc))


def run(argv: list[str] | None = None) -> int:
    """Entry point: parse args, bootstrap Spark, process each table in order."""
    job = _job_common.job_name_from_file(__file__)
    args = _job_common.parse_job_args(job, argv)
    cfg, ctx, spark = _job_common.bootstrap(job, args)

    from src.common import logging_util
    from src.common.audit import STATUS_FAILED, AuditRecord
    from src.common import lineage

    tables = _job_common.tables_to_process(args)
    for table in tables:
        try:
            process_table(spark, cfg, ctx, args.run_id, table, args.local)
        except Exception as exc:  # noqa: BLE001 - top-level fatal for this table
            # Non-DQ fatal error -> write a FAILED audit row, then non-zero exit.
            logger = logging_util.get_logger(args.run_id, "bronze", table)
            logging_util.log_event(logger, "read_fail", message=str(exc), table=table)
            now = lineage._now_iso()
            record = AuditRecord(
                run_id=args.run_id, job=job, layer_from="landing", layer_to="bronze",
                table_name=table, status=STATUS_FAILED, input_rows=0,
                dq_passed=False, dq_critical_failed=0,
                started_at=now, ended_at=now, duration_ms=0,
            )
            _safe_write_audit(spark, record, ctx, logger)
            return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - requires Spark
    sys.exit(run())
