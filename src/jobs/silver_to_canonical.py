"""silver_to_canonical.py -- standardize + MASK PII -> Iceberg (design S6.3/S4).

Stage 3. Silver holds cleansed-but-UNMASKED data; canonical is the FIRST masked
layer, and gold reads only from canonical, so "PII never reaches gold unmasked"
holds structurally.

Responsibilities per table:
  * Read silver Iceberg.
  * Standardize canonical code values and date/timestamp formats (POC: codes are
    already upper-cased in silver; this is where a demo would map synonyms).
  * Apply masking via ``common.masking.apply_masking`` (design S4):
      card_number -> last-4, email -> partial, national_id -> SHA-256+salt token
      IN PLACE under its own name, names -> first char, phone -> last 2,
      date_of_birth -> REPLACED by birth_year:int.
    Masked columns keep their ORIGINAL names/types -- NO ``_masked`` suffix at
    canonical (design S6.3 Finding 4); the suffix only appears in gold marts.
  * Write Iceberg ``glue_catalog.ocbc_poc_dev_canonical.<table>``.
  * Lineage ``column_mappings`` carry ``transform='mask:<method>'`` on each PII
    column so masking is visible in lineage.
  * One SUCCESS audit row per table (FAILED on a non-DQ fatal via top-level except).

Salt precedence is resolved ONCE at job start (design S4/S10):
    env PIPELINE_MASK_SALT > config.masking.salt > hard startup fail.

Spark/boto3 imports are LAZY.
"""

from __future__ import annotations

import os
import pathlib
import sys
import time

# Support BOTH invocation styles (module and bare script / EMR entryPoint).
if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    from src.jobs import _job_common
else:
    from . import _job_common


def resolve_salt(cfg) -> str:
    """Resolve the masking salt with the design S4 precedence, or fail hard.

    env ``PIPELINE_MASK_SALT`` (if non-empty) > ``config.masking.salt`` > raise.
    This runs at JOB START so a misconfigured salt fails fast before Spark work.
    """
    from src.common.config_loader import ConfigError

    env_salt = os.environ.get("PIPELINE_MASK_SALT")
    if env_salt and env_salt.strip():
        return env_salt
    cfg_salt = (cfg.masking or {}).get("salt")
    if cfg_salt and str(cfg_salt).strip():
        return str(cfg_salt)
    raise ConfigError(
        "event=config_fail no masking salt: set PIPELINE_MASK_SALT or config.masking.salt"
    )


def process_table(spark, cfg, ctx, run_id: str, table: str, salt: str, local: bool) -> None:
    """Read silver, standardize + mask, write canonical Iceberg."""
    from src.common import io_paths, lineage, logging_util, masking
    from src.common.audit import STATUS_SUCCESS, AuditRecord

    job = "silver_to_canonical"
    logger = logging_util.get_logger(run_id, "canonical", table)
    started_at = lineage._now_iso()
    start_monotonic = time.monotonic()

    silver_fq = io_paths.iceberg_table("silver", table, ctx)
    logging_util.log_event(logger, logging_util.EVENT_JOB_START, table=table)

    silver = spark.table(silver_fq)
    input_rows = silver.count()
    logging_util.log_event(logger, logging_util.EVENT_READ_DONE, row_count=input_rows)

    # Drop the bronze audit columns -- canonical carries the business schema only.
    for audit_col in ("_ingested_at", "_run_id", "_source_file"):
        if audit_col in silver.columns:
            silver = silver.drop(audit_col)

    # Apply PII masking per the S4 catalog (in place; dob -> birth_year).
    masked = masking.apply_masking(silver, table, salt)

    output_rows = masked.count()
    fq_table = io_paths.iceberg_table("canonical", table, ctx)
    with logging_util.timed(logger, "canonical_write", table=table):
        masked.writeTo(fq_table).using("iceberg").createOrReplace()
    logging_util.log_event(logger, logging_util.EVENT_WRITE_DONE, row_count=output_rows)

    # --- lineage with mask:<method> transforms on PII columns -----------------
    builder = lineage.start_lineage(run_id, job, "silver", "canonical", table)
    builder.add_input(
        silver_fq, "iceberg", row_count=input_rows,
        glue_table=f"{io_paths.glue_db('silver', ctx)}.{table}",
    )
    builder.add_output(
        fq_table, "iceberg", row_count=output_rows,
        glue_table=f"{io_paths.glue_db('canonical', ctx)}.{table}",
    )
    for column, method in masking.PII_COLUMNS.get(table, {}).items():
        # date_of_birth becomes birth_year; reflect that in the mapping output.
        out_col = "birth_year" if method == "dob" else column
        builder.add_column_mapping(out_col, [column], f"mask:{method}")
    builder.set_metrics(input_rows=input_rows, output_rows=output_rows)
    lineage_path = _emit_lineage(builder, local, ctx)

    ended_at = lineage._now_iso()
    duration_ms = int((time.monotonic() - start_monotonic) * 1000)
    record = AuditRecord(
        run_id=run_id, job=job, layer_from="silver", layer_to="canonical",
        table_name=table, status=STATUS_SUCCESS, input_rows=input_rows,
        output_rows=output_rows, rejected_rows=0, dq_passed=True,
        dq_critical_failed=0, started_at=started_at, ended_at=ended_at,
        duration_ms=duration_ms, lineage_path=lineage_path,
    )
    _safe_write_audit(spark, record, ctx, logger)
    logging_util.log_event(logger, logging_util.EVENT_JOB_END, table=table, status=STATUS_SUCCESS)


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
    """Entry point: resolve the salt FIRST, then process each table in order."""
    job = _job_common.job_name_from_file(__file__)
    args = _job_common.parse_job_args(job, argv)
    cfg, ctx, spark = _job_common.bootstrap(job, args)

    from src.common import lineage, logging_util
    from src.common.audit import STATUS_FAILED, AuditRecord

    # Resolve the salt at startup (hard fail if absent -- design S4/S10).
    salt = resolve_salt(cfg)

    tables = _job_common.tables_to_process(args)
    for table in tables:
        try:
            process_table(spark, cfg, ctx, args.run_id, table, salt, args.local)
        except Exception as exc:  # noqa: BLE001 - non-DQ fatal error
            logger = logging_util.get_logger(args.run_id, "canonical", table)
            logging_util.log_event(logger, "write_fail", message=str(exc), table=table)
            now = lineage._now_iso()
            record = AuditRecord(
                run_id=args.run_id, job=job, layer_from="silver", layer_to="canonical",
                table_name=table, status=STATUS_FAILED, input_rows=0,
                dq_passed=False, dq_critical_failed=0,
                started_at=now, ended_at=now, duration_ms=0,
            )
            _safe_write_audit(spark, record, ctx, logger)
            return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - requires Spark
    sys.exit(run())
