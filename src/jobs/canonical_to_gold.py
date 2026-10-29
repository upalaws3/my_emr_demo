"""canonical_to_gold.py -- aggregate marts -> Iceberg (design S6.4).

Stage 4. Reads ONLY canonical (never silver), so all PII-derived values are
already masked. Builds three gold marts, each its own Iceberg table in
``ocbc_poc_dev_gold``:

  * ``customer_360``       one row per customer_id, assembled from SEPARATE
                           per-customer aggregations joined on customer_id (NOT a
                           flat join -- design S6.4 Finding 3).
  * ``daily_txn_summary``  grain account_id x txn_date x currency.
  * ``account_balances``   one row per account_id, native currency, with the
                           customer_token (canonical customers.national_id token).

Money aggregates stay PER CURRENCY (no FX conversion -- design S6.4).

CLI note: the shared contract is table-centric (--all/--table), but gold emits
MARTS, not the four source tables. ``--all`` (the default) builds all three
marts. ``--table`` is a dev convenience accepting a MART name; an unknown value
builds all marts. Each mart emits lineage + one SUCCESS audit row.

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

# The three marts this stage produces (design S6.4).
GOLD_MARTS = ("customer_360", "daily_txn_summary", "account_balances")


def _canonical(spark, ctx, table: str):
    """Read a canonical Iceberg table as a DataFrame."""
    from src.common import io_paths

    return spark.table(io_paths.iceberg_table("canonical", table, ctx))


def build_customer_360(spark, ctx):
    """Build customer_360 from SEPARATE per-customer aggregations (S6.4 Finding 3).

    Each metric is computed at its correct grain and keyed by customer_id, then
    the per-metric results are JOINED on customer_id. We deliberately do NOT
    flat-join customers x accounts x cards x transactions (that fans out rows and
    corrupts txn_count / last_txn_at).
    """
    from pyspark.sql import functions as F

    customers = _canonical(spark, ctx, "customers")
    accounts = _canonical(spark, ctx, "accounts")
    cards = _canonical(spark, ctx, "cards")
    transactions = _canonical(spark, ctx, "transactions")

    # account metrics from accounts grouped by customer_id.
    acct_metrics = accounts.groupBy("customer_id").agg(
        F.countDistinct("account_id").cast("int").alias("account_count"),
        F.sum(F.when(F.col("status") == F.lit("ACTIVE"), 1).otherwise(0))
        .cast("int")
        .alias("active_account_count"),
    )

    # card_count from accounts JOIN cards (on account_id) grouped by customer_id.
    acct_cards = accounts.select("account_id", "customer_id").join(
        cards.select("account_id", "card_id"), on="account_id", how="inner"
    )
    card_metrics = acct_cards.groupBy("customer_id").agg(
        F.countDistinct("card_id").cast("int").alias("card_count")
    )

    # txn metrics from accounts JOIN transactions (on account_id) by customer_id.
    acct_txns = accounts.select("account_id", "customer_id").join(
        transactions.select("account_id", "txn_timestamp"), on="account_id", how="inner"
    )
    txn_metrics = acct_txns.groupBy("customer_id").agg(
        F.count(F.lit(1)).cast("bigint").alias("txn_count"),
        F.max("txn_timestamp").alias("last_txn_at"),
    )

    # Start from the customer dimension so every customer appears once.
    mart = (
        customers.select(
            "customer_id",
            F.col("first_name").alias("first_name_masked"),
            F.col("last_name").alias("last_name_masked"),
            F.col("email").alias("email_masked"),
            "country",
        )
        .join(acct_metrics, on="customer_id", how="left")
        .join(card_metrics, on="customer_id", how="left")
        .join(txn_metrics, on="customer_id", how="left")
    )

    # Fill count nulls (customers with no accounts/cards/txns) with zero.
    mart = mart.fillna(
        {"account_count": 0, "active_account_count": 0, "card_count": 0, "txn_count": 0}
    )
    # Column order per the S6.4 mart table.
    return mart.select(
        "customer_id", "first_name_masked", "last_name_masked", "email_masked", "country",
        "account_count", "card_count", "active_account_count", "txn_count", "last_txn_at",
    )


def build_daily_txn_summary(spark, ctx):
    """Build daily_txn_summary (grain account_id x txn_date x currency)."""
    from pyspark.sql import functions as F

    transactions = _canonical(spark, ctx, "transactions")
    base = transactions.withColumn("txn_date", F.to_date(F.col("txn_timestamp")))
    grouped = base.groupBy("account_id", "txn_date", "currency").agg(
        F.count(F.lit(1)).cast("bigint").alias("txn_count"),
        F.sum(F.when(F.col("txn_type") == F.lit("DEBIT"), F.col("amount")).otherwise(F.lit(0)))
        .cast("decimal(18,2)")
        .alias("debit_total"),
        F.sum(F.when(F.col("txn_type") == F.lit("CREDIT"), F.col("amount")).otherwise(F.lit(0)))
        .cast("decimal(18,2)")
        .alias("credit_total"),
    )
    grouped = grouped.withColumn(
        "net_amount", (F.col("credit_total") - F.col("debit_total")).cast("decimal(18,2)")
    )
    return grouped.select(
        "account_id", "txn_date", "currency", "txn_count", "debit_total", "credit_total",
        "net_amount",
    )


def build_account_balances(spark, ctx):
    """Build account_balances (one row per account_id, native currency).

    ``customer_token`` is the canonical ``customers.national_id`` token joined via
    customer_id and renamed to customer_token in gold.
    """
    from pyspark.sql import functions as F

    accounts = _canonical(spark, ctx, "accounts")
    customers = _canonical(spark, ctx, "customers")

    tokens = customers.select(
        "customer_id", F.col("national_id").alias("customer_token")
    )
    mart = accounts.join(tokens, on="customer_id", how="left")
    return mart.select(
        "account_id", "customer_id", "customer_token", "account_type", "currency", "balance",
        "status",
    )


# Mart name -> builder function + the lineage input tables it reads.
_MART_BUILDERS = {
    "customer_360": (build_customer_360, ["customers", "accounts", "cards", "transactions"]),
    "daily_txn_summary": (build_daily_txn_summary, ["transactions"]),
    "account_balances": (build_account_balances, ["accounts", "customers"]),
}


def process_mart(spark, cfg, ctx, run_id: str, mart: str, local: bool) -> None:
    """Build one gold mart, write Iceberg, emit lineage + SUCCESS audit row."""
    from src.common import io_paths, lineage, logging_util
    from src.common.audit import STATUS_SUCCESS, AuditRecord

    job = "canonical_to_gold"
    logger = logging_util.get_logger(run_id, "gold", mart)
    started_at = lineage._now_iso()
    start_monotonic = time.monotonic()

    builder_fn, input_tables = _MART_BUILDERS[mart]
    logging_util.log_event(logger, logging_util.EVENT_JOB_START, table=mart)

    df = builder_fn(spark, ctx)
    output_rows = df.count()

    fq_table = io_paths.iceberg_table("gold", mart, ctx)
    with logging_util.timed(logger, "gold_write", table=mart):
        df.writeTo(fq_table).using("iceberg").createOrReplace()
    logging_util.log_event(logger, logging_util.EVENT_WRITE_DONE, row_count=output_rows)

    # --- lineage: multi-input -> single-output, aggregation transforms --------
    bld = lineage.start_lineage(run_id, job, "canonical", "gold", mart)
    for src_table in input_tables:
        bld.add_input(
            io_paths.iceberg_table("canonical", src_table, ctx),
            "iceberg",
            glue_table=f"{io_paths.glue_db('canonical', ctx)}.{src_table}",
        )
    bld.add_output(
        fq_table, "iceberg", row_count=output_rows,
        glue_table=f"{io_paths.glue_db('gold', ctx)}.{mart}",
    )
    # A couple of representative aggregation column mappings (design S6.4).
    if mart == "customer_360":
        bld.add_column_mapping("txn_count", ["transactions.transaction_id"], "agg:count")
        bld.add_column_mapping("last_txn_at", ["transactions.txn_timestamp"], "agg:max")
    elif mart == "daily_txn_summary":
        bld.add_column_mapping("net_amount", ["transactions.amount", "transactions.txn_type"], "agg:sum")
    elif mart == "account_balances":
        bld.add_column_mapping("customer_token", ["customers.national_id"], "join")
    lineage_path = _emit_lineage(bld, local, ctx)

    ended_at = lineage._now_iso()
    duration_ms = int((time.monotonic() - start_monotonic) * 1000)
    record = AuditRecord(
        run_id=run_id, job=job, layer_from="canonical", layer_to="gold",
        table_name=mart, status=STATUS_SUCCESS, input_rows=output_rows,
        output_rows=output_rows, rejected_rows=0, dq_passed=True,
        dq_critical_failed=0, started_at=started_at, ended_at=ended_at,
        duration_ms=duration_ms, lineage_path=lineage_path,
    )
    _safe_write_audit(spark, record, ctx, logger)
    logging_util.log_event(logger, logging_util.EVENT_JOB_END, table=mart, status=STATUS_SUCCESS)


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


def _marts_to_build(args) -> tuple[str, ...]:
    """Resolve which marts to build. --table may name a single mart; else all."""
    if args.table is not None and args.table in GOLD_MARTS:
        return (args.table,)
    return GOLD_MARTS


def run(argv: list[str] | None = None) -> int:
    """Entry point: build the gold marts; one audit row per mart."""
    job = _job_common.job_name_from_file(__file__)
    # Gold accepts MART names via --table, so parse without the table-name guard:
    # reuse the shared parser but tolerate a mart name (not in TABLE_ORDER).
    args = _parse_gold_args(job, argv)
    cfg, ctx, spark = _job_common.bootstrap(job, args)

    from src.common import lineage, logging_util
    from src.common.audit import STATUS_FAILED, AuditRecord

    for mart in _marts_to_build(args):
        try:
            process_mart(spark, cfg, ctx, args.run_id, mart, args.local)
        except Exception as exc:  # noqa: BLE001 - non-DQ fatal error
            logger = logging_util.get_logger(args.run_id, "gold", mart)
            logging_util.log_event(logger, "write_fail", message=str(exc), table=mart)
            now = lineage._now_iso()
            record = AuditRecord(
                run_id=args.run_id, job=job, layer_from="canonical", layer_to="gold",
                table_name=mart, status=STATUS_FAILED, input_rows=0,
                dq_passed=False, dq_critical_failed=0,
                started_at=now, ended_at=now, duration_ms=0,
            )
            _safe_write_audit(spark, record, ctx, logger)
            return 1
    return 0


def _parse_gold_args(job: str, argv: list[str] | None):
    """Parse the shared CLI but allow --table to name a gold MART.

    Mirrors ``_job_common.parse_job_args`` (same --run-id/--env/--all/--table/
    --local contract, same config_fail behavior) but validates --table against
    the gold mart names instead of the source table names.
    """
    import argparse

    parser = argparse.ArgumentParser(prog=job, description=f"{job} stage.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env", required=True, choices=("dev",))
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", dest="all_tables", action="store_true")
    group.add_argument("--table", dest="table", default=None)
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args(argv)
    if not args.run_id or not args.run_id.strip():
        parser.exit(2, "event=config_fail --run-id must be a non-empty string\n")
    if not args.all_tables and args.table is None:
        args.all_tables = True
    if args.table is not None and args.table not in GOLD_MARTS:
        parser.exit(2, f"event=config_fail unknown --table {args.table!r}; expected {GOLD_MARTS}\n")
    return args


if __name__ == "__main__":  # pragma: no cover - requires Spark
    sys.exit(run())
