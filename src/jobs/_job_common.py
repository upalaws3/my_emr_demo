"""_job_common.py -- shared CLI + bootstrap for the four transition jobs.

The four stage scripts (``landing_to_bronze``, ``bronze_to_silver``,
``silver_to_canonical``, ``canonical_to_gold``) share one CLI contract and one
startup sequence (design S6/S8/S10). Factoring it here keeps each job focused on
its transform while guaranteeing all four behave identically at the edges:

CLI contract (design S6 -- authoritative):
  --run-id <id>   required, non-empty string
  --env <env>     required, allowed set {dev}
  --all           process all four tables in the FIXED order (DEFAULT behavior)
  --table <name>  optional, dev-only single-table; MUTUALLY EXCLUSIVE with --all;
                  NOT passed by Step Functions
  --local         optional flag used only by the local Spark smoke test
  (there is NO --job: the job name is derived from the script filename)

Invalid args -> non-zero exit tagged ``config_fail`` (design S10 input validation).

Spark/boto3/config imports stay LAZY (inside functions) so these modules
byte-compile and import on the bare host.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

# Make ``src``/``data`` importable when a job is run directly as
# ``python src/jobs/<job>.py`` (EMR sets entryPoint to the script path too).
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# The FIXED medallion processing order (parents before children). Importing it
# from schema_defs keeps a single source of truth.
from data.generator.schema_defs import TABLE_ORDER  # noqa: E402

# Allowed --env values for the POC (design S6/S10).
_ALLOWED_ENVS = ("dev",)


def job_name_from_file(file_path: str) -> str:
    """Derive the job/stage name from a script filename (design S6: no --job).

    e.g. ``.../src/jobs/bronze_to_silver.py`` -> ``bronze_to_silver``.
    """
    return pathlib.Path(file_path).stem


def parse_job_args(job: str, argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the shared job CLI; exit non-zero (config_fail) on invalid args.

    ``--all`` and ``--table`` are mutually exclusive; ``--all`` is the default
    when neither is given. ``--run-id`` must be non-empty and ``--env`` must be
    in the allowed set. argparse already exits code 2 on bad args; we add our own
    validation (empty run-id) with a ``config_fail`` tag so the failure reason is
    greppable, matching the design S10 contract.
    """
    parser = argparse.ArgumentParser(
        prog=job,
        description=f"{job} stage of the OCBC medallion pipeline.",
    )
    parser.add_argument("--run-id", required=True, help="Non-empty run identifier.")
    parser.add_argument("--env", required=True, choices=_ALLOWED_ENVS, help="Environment (dev).")
    # --all / --table are mutually exclusive; --all is the default behavior.
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--all",
        dest="all_tables",
        action="store_true",
        help="Process all tables in the fixed order (default).",
    )
    group.add_argument(
        "--table",
        dest="table",
        default=None,
        help="Dev-only: process a single table (not used by Step Functions).",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use a local Spark/Iceberg catalog (smoke test only).",
    )

    args = parser.parse_args(argv)

    # Non-empty run-id (argparse cannot express "non-empty"); tag config_fail.
    if not args.run_id or not args.run_id.strip():
        parser.exit(2, "event=config_fail --run-id must be a non-empty string\n")

    # Default to --all when neither --all nor --table was provided.
    if not args.all_tables and args.table is None:
        args.all_tables = True

    # Validate --table names against the known table set.
    if args.table is not None and args.table not in TABLE_ORDER:
        parser.exit(2, f"event=config_fail unknown --table {args.table!r}; expected {TABLE_ORDER}\n")

    return args


def tables_to_process(args: argparse.Namespace) -> tuple[str, ...]:
    """Return the ordered tuple of tables to process for this invocation."""
    if args.table is not None:
        return (args.table,)
    return TABLE_ORDER


def build_io_context(cfg):
    """Build ONE io_paths.IoContext from config (threaded through the whole job).

    The datalake bucket's account-id suffix is unknown on the bare host, so for
    POC runs we fall back to the io_paths default bucket placeholder unless the
    env var ``PIPELINE_ACCOUNT_ID`` is set. The Glue DB prefix is deterministic
    from project+env (design S2). FEAT-001 Finding 1 established this pattern.
    """
    import os

    from src.common import io_paths

    account_id = os.environ.get("PIPELINE_ACCOUNT_ID")
    db_prefix = f"{cfg.project.replace('-', '_')}_{cfg.env}"
    if account_id:
        datalake_bucket = f"{cfg.buckets['datalake']}-{account_id}"
        return io_paths.IoContext(datalake_bucket=datalake_bucket, db_prefix=db_prefix)
    # No account id available (local/smoke): keep the default bucket placeholder
    # but still use the real db prefix so table identifiers are correct.
    return io_paths.IoContext(db_prefix=db_prefix)


def bootstrap(job: str, args: argparse.Namespace):
    """Load config, build the IoContext and a Spark session; return the trio.

    Spark import is lazy (inside ``build_spark``). ``--local`` swaps to a local
    Hadoop Iceberg catalog for the smoke test.
    """
    from src.common import config_loader, spark_session

    cfg = config_loader.load_config()
    ctx = build_io_context(cfg)

    warehouse = None if args.local else None  # warehouse set by Glue on AWS
    spark = spark_session.build_spark(app_name=f"ocbc-{job}", local=args.local)
    return cfg, ctx, spark
