"""bronze_to_silver.py -- cleanse + DATA-QUALITY gate -> Iceberg (design S6.2).

Stage 2 and the ONLY place the DQ FAIL gate lives. Responsibilities per table,
processed in the FIXED order customers -> accounts -> cards -> transactions
(parents before children so referential checks are well-defined):

  1. Read bronze Parquet.
  2. Cleanse: trim strings, normalize coded-column case, drop exact duplicates
     with a full-row ``distinct()``, THEN PK-dedupe keeping the latest
     ``_ingested_at``.
  3. Build the DQ expectations from config and resolve referential parents from
     the JUST-WRITTEN silver parent (``ref_dfs={parent_name: parent_df}``).
  4. Run the EXACT S6.2a sequence:
        result = run_expectations(...)
        emit_quality(result)          # ALWAYS before the gate
        try: enforce_gate(result)
        except QualityGateError: write ONE DQ_FAILED audit row, then re-raise
  5. On pass: write Iceberg ``glue_catalog.ocbc_poc_dev_silver.<table>``
     (create-if-not-exists then overwrite-by-table), emit lineage, write ONE
     SUCCESS audit row.

Spark/boto3 imports are LAZY.
"""

from __future__ import annotations

import pathlib
import sys
import time

# Support BOTH invocation styles (module and bare script / EMR entryPoint).
if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    from src.jobs import _job_common
else:
    from . import _job_common

# Columns whose case we normalize (coded/enumerated columns) per table. Codes
# are upper-cased; free text and ids are left as-is (design S6.2 cleanse).
_CODED_COLUMNS = {
    "customers": ["country"],
    "accounts": ["account_type", "currency", "status"],
    "cards": ["card_type", "status"],
    "transactions": ["currency", "txn_type"],
}

# Primary key per table (for the PK dedupe + unique check).
_PRIMARY_KEY = {
    "customers": "customer_id",
    "accounts": "account_id",
    "cards": "card_id",
    "transactions": "transaction_id",
}

# Referential parent per child table: (child_fk_col, parent_table, parent_col).
_REFERENTIAL = {
    "accounts": ("customer_id", "customers", "customer_id"),
    "cards": ("account_id", "accounts", "account_id"),
    "transactions": ("account_id", "accounts", "account_id"),
}

# in_set column -> reference_data key. Small, explicit map (NOT a generic engine)
# so an in_set DQ rule resolves its allowed values from the single source of
# truth in config.reference_data without repeating the literal set per rule.
_IN_SET_REFERENCE_KEY = {
    "country": "countries",
    "currency": "currencies",
    "status": "account_statuses",
    "card_type": "card_types",
    "txn_type": "txn_types",
}


def _REFERENCE_ALLOWED(cfg, column: str) -> list:
    """Resolve an in_set rule's allowed values from config.reference_data."""
    key = _IN_SET_REFERENCE_KEY.get(column)
    if key is None:
        return []
    return list(cfg.reference_data.get(key, []))


def _build_expectations(cfg, table: str):
    """Translate config/pipeline.yaml dq_rules[table] into Expectation objects.

    Reads the declarative rules (kind/column(s)/params/severity) so the gate is
    retunable from config without code changes (design S6.5).
    """
    from src.common import quality

    rules = cfg.dq_rules.get(table, {})
    exps = []
    for entry in rules.get("expectations", []):
        kind = entry["kind"]
        severity = entry.get("severity", quality.SEVERITY_CRITICAL)
        params = entry.get("params", {}) or {}
        if kind == "not_null":
            exps.append(quality.expect_not_null(entry["column"], severity))
        elif kind == "unique":
            exps.append(quality.expect_unique(entry["columns"], severity))
        elif kind == "in_set":
            # Allowed sets live ONCE in config.reference_data (design S6.5); a
            # rule may still pin an inline params.allowed, but normally the set
            # is resolved from reference_data by a small column -> key map.
            allowed = params.get("allowed")
            if allowed is None:
                allowed = _REFERENCE_ALLOWED(cfg, entry["column"])
            exps.append(quality.expect_in_set(entry["column"], allowed or [], severity))
        elif kind == "range":
            exps.append(
                quality.expect_range(entry["column"], params.get("min"), params.get("max"), severity)
            )
        elif kind == "referential":
            exps.append(
                quality.expect_referential(
                    entry["column"], params["ref_table"], params["ref_col"], severity
                )
            )
        elif kind == "row_count":
            exps.append(quality.expect_row_count(params.get("min", 1), params.get("max"), severity))
        else:  # pragma: no cover - config typo guard
            raise ValueError(f"unknown dq kind {kind!r} for {table}")
    return exps


def _cleanse(df, table: str):
    """Trim strings, upper-case coded columns, drop exact dupes, PK-dedupe."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cleansed = df
    # Trim all string columns.
    for field in cleansed.schema.fields:
        if field.dataType.typeName() == "string":
            cleansed = cleansed.withColumn(field.name, F.trim(F.col(field.name)))
    # Upper-case coded columns.
    for col in _CODED_COLUMNS.get(table, []):
        if col in cleansed.columns:
            cleansed = cleansed.withColumn(col, F.upper(F.col(col)))

    # Full-row distinct() FIRST (drops exact duplicates). The injected DQ-failure
    # rows differ in a non-key column so they SURVIVE this step (design S6.5).
    cleansed = cleansed.distinct()

    # PK dedupe keeping latest _ingested_at. Two rows identical on PK AND
    # _ingested_at cannot be disambiguated, so both survive (the planted dup).
    pk = _PRIMARY_KEY[table]
    if "_ingested_at" in cleansed.columns:
        w = Window.partitionBy(pk).orderBy(F.col("_ingested_at").desc())
        cleansed = (
            cleansed.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    return cleansed


def process_table(spark, cfg, ctx, run_id: str, table: str, local: bool) -> None:
    """Read bronze, cleanse, run the DQ gate, and write silver on pass."""
    from src.common import io_paths, lineage, logging_util, quality
    from src.common.audit import STATUS_DQ_FAILED, STATUS_SUCCESS, AuditRecord

    job = "bronze_to_silver"
    logger = logging_util.get_logger(run_id, "silver", table)
    started_at = lineage._now_iso()
    start_monotonic = time.monotonic()

    bronze_prefix = io_paths.s3_layer_prefix("bronze", table, ctx)
    logging_util.log_event(logger, logging_util.EVENT_JOB_START, table=table)

    bronze = spark.read.parquet(bronze_prefix)
    input_rows = bronze.count()
    logging_util.log_event(logger, logging_util.EVENT_READ_DONE, row_count=input_rows)

    cleansed = _cleanse(bronze, table)

    # --- resolve referential parents from the JUST-WRITTEN silver parent ------
    ref_dfs = {}
    if table in _REFERENTIAL:
        _, parent_table, _ = _REFERENTIAL[table]
        ref_dfs[parent_table] = spark.table(io_paths.iceberg_table("silver", parent_table, ctx))

    expectations = _build_expectations(cfg, table)

    # ===================== DQ sequence (design S6.2a) =====================
    logging_util.log_event(logger, logging_util.EVENT_DQ_START, table=table)
    result = quality.run_expectations(cleansed, expectations, run_id, job, table, ref_dfs)
    quality_path = _emit_quality(result, local, ctx)  # ALWAYS before the gate

    try:
        quality.enforce_gate(result)
    except quality.QualityGateError:
        # DQ_FAILED audit row for the failing table, written BEFORE re-raising
        # (design S6.2a). Post-write fields stay None; input_rows IS populated.
        logging_util.log_event(logger, logging_util.EVENT_DQ_FAIL, table=table)
        ended_at = lineage._now_iso()
        duration_ms = int((time.monotonic() - start_monotonic) * 1000)
        record = AuditRecord(
            run_id=run_id, job=job, layer_from="bronze", layer_to="silver",
            table_name=result.table, status=STATUS_DQ_FAILED,
            input_rows=input_rows, dq_passed=False,
            dq_critical_failed=result.critical_failed,
            started_at=started_at, ended_at=ended_at, duration_ms=duration_ms,
            quality_path=quality_path,
        )
        _safe_write_audit(spark, record, ctx, logger)
        raise  # non-zero exit -> SFN Catch[States.ALL] -> PipelineFailed
    logging_util.log_event(logger, logging_util.EVENT_DQ_PASS, table=table)
    # ======================================================================

    # --- write silver Iceberg (create-if-not-exists, overwrite-by-table) ------
    output_rows = cleansed.count()
    fq_table = io_paths.iceberg_table("silver", table, ctx)
    with logging_util.timed(logger, "silver_write", table=table):
        cleansed.writeTo(fq_table).using("iceberg").createOrReplace()
    logging_util.log_event(logger, logging_util.EVENT_WRITE_DONE, row_count=output_rows)

    # --- lineage + SUCCESS audit row ------------------------------------------
    builder = lineage.start_lineage(run_id, job, "bronze", "silver", table)
    builder.add_input(
        bronze_prefix, "parquet", row_count=input_rows,
        glue_table=f"{io_paths.glue_db('bronze', ctx)}.{table}",
    )
    builder.add_output(
        fq_table, "iceberg", row_count=output_rows,
        glue_table=f"{io_paths.glue_db('silver', ctx)}.{table}",
    )
    builder.set_metrics(input_rows=input_rows, output_rows=output_rows, rejected_rows=0)
    lineage_path = _emit_lineage(builder, local, ctx)

    ended_at = lineage._now_iso()
    duration_ms = int((time.monotonic() - start_monotonic) * 1000)
    record = AuditRecord(
        run_id=run_id, job=job, layer_from="bronze", layer_to="silver",
        table_name=table, status=STATUS_SUCCESS, input_rows=input_rows,
        output_rows=output_rows, rejected_rows=0, dq_passed=True,
        dq_critical_failed=0, started_at=started_at, ended_at=ended_at,
        duration_ms=duration_ms, lineage_path=lineage_path, quality_path=quality_path,
    )
    _safe_write_audit(spark, record, ctx, logger)
    logging_util.log_event(logger, logging_util.EVENT_JOB_END, table=table, status=STATUS_SUCCESS)


def _emit_quality(result, local: bool, ctx) -> str | None:
    """Emit quality JSON; best-effort but ALWAYS attempted before the gate."""
    from src.common import logging_util, quality

    logger = logging_util.get_logger(result.run_id, "silver", result.table)
    try:
        if local:
            return quality.emit_quality(result, local_dir="./_quality_out", ctx=ctx)
        import boto3

        return quality.emit_quality(result, s3_client=boto3.client("s3"), ctx=ctx)
    except Exception as exc:  # noqa: BLE001
        logging_util.log_event(logger, "quality_emit_fail", message=str(exc))
        return None


def _emit_lineage(builder, local: bool, ctx) -> str | None:
    from src.common import lineage, logging_util

    logger = logging_util.get_logger(builder.run_id, builder.layer_to, builder.table)
    try:
        if local:
            return lineage.emit(builder, local_dir="./_lineage_out", ctx=ctx)
        import boto3

        return lineage.emit(builder, s3_client=boto3.client("s3"), ctx=ctx)
    except Exception as exc:  # noqa: BLE001
        logging_util.log_event(logger, "lineage_emit_fail", message=str(exc))
        return None


def _safe_write_audit(spark, record, ctx, logger) -> None:
    from src.common import io_paths, logging_util
    from src.common.audit import write_audit

    try:
        write_audit(spark, record, ctx.catalog, io_paths.glue_db("audit", ctx), ctx=ctx)
    except Exception as exc:  # noqa: BLE001
        logging_util.log_event(logger, "audit_write_fail", message=str(exc))


def run(argv: list[str] | None = None) -> int:
    """Entry point: parse args, bootstrap, process each table in fixed order.

    A QualityGateError (critical DQ failure) propagates as a non-zero exit AFTER
    its DQ_FAILED audit row was written inside ``process_table``. Any other fatal
    error writes a FAILED audit row here before exiting non-zero.
    """
    job = _job_common.job_name_from_file(__file__)
    args = _job_common.parse_job_args(job, argv)
    cfg, ctx, spark = _job_common.bootstrap(job, args)

    from src.common import lineage, logging_util, quality
    from src.common.audit import STATUS_FAILED, AuditRecord

    tables = _job_common.tables_to_process(args)
    for table in tables:
        try:
            process_table(spark, cfg, ctx, args.run_id, table, args.local)
        except quality.QualityGateError:
            # DQ_FAILED audit row was already written in process_table (S6.2a).
            return 1
        except Exception as exc:  # noqa: BLE001 - non-DQ fatal error
            logger = logging_util.get_logger(args.run_id, "silver", table)
            logging_util.log_event(logger, "write_fail", message=str(exc), table=table)
            now = lineage._now_iso()
            record = AuditRecord(
                run_id=args.run_id, job=job, layer_from="bronze", layer_to="silver",
                table_name=table, status=STATUS_FAILED, input_rows=0,
                dq_passed=False, dq_critical_failed=0,
                started_at=now, ended_at=now, duration_ms=0,
            )
            _safe_write_audit(spark, record, ctx, logger)
            return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - requires Spark
    sys.exit(run())
