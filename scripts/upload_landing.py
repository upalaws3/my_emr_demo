"""upload_landing.py -- push generated landing files to the S3 landing prefix.

Design S15 / AC2. This is a small operator convenience: after
``data.generator.generate`` writes the four landing files locally, this script
uploads each one to ``s3://<datalake>/landing/<table>/`` using the SAME path
layout that ``src/common/io_paths.py`` produces, so bronze reads exactly where
upload wrote.

IMPORTANT -- where this runs: NOT on the local authoring host (which has no AWS
access). It is intended to run from an operator workstation or a CI job that DOES
have AWS credentials and network reach. On this host it only needs to
byte-compile and import, so ``boto3`` is imported LAZILY inside ``upload`` -- the
module itself has no hard AWS dependency.

Usage (on a host with AWS creds)::

    python scripts/upload_landing.py --src ./_local_out --env dev --account-id 123456789012

``--account-id`` is required because the bucket name carries the account id
(design S2.2); without it we cannot build the real bucket name.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

# Make ``src`` importable when this script is run directly (python scripts/...).
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.common import config_loader, io_paths  # noqa: E402  (after sys.path tweak)

# The four landing files and the table each maps to (design S3). Order matches
# the medallion processing order but upload order is irrelevant.
_LANDING_FILES = {
    "customers": "customers.csv",
    "accounts": "accounts.csv",
    "cards": "cards.csv",
    "transactions": "transactions.json",
}


def _build_ctx(account_id: str) -> io_paths.IoContext:
    """Build an IoContext with the real bucket name (base name + account id).

    Mirrors the thread-one-context pattern FEAT-001 established: the datalake
    bucket is ``<config.buckets.datalake>-<account_id>`` and the Glue DB prefix
    is ``<project with dashes->underscores>_<env>`` (design S2).
    """
    cfg = config_loader.load_config()
    datalake_bucket = f"{cfg.buckets['datalake']}-{account_id}"
    db_prefix = f"{cfg.project.replace('-', '_')}_{cfg.env}"
    return io_paths.IoContext(datalake_bucket=datalake_bucket, db_prefix=db_prefix)


def upload(src_dir: str, account_id: str, env: str = "dev") -> list[str]:
    """Upload each generated landing file to its S3 landing prefix.

    Returns the list of S3 URIs written. ``boto3`` is imported lazily so this
    module stays import-safe on a host without AWS.
    """
    # Lazy import -- only needed when actually uploading (not on the bare host).
    import boto3

    ctx = _build_ctx(account_id)
    src = pathlib.Path(src_dir)
    s3 = boto3.client("s3")

    written: list[str] = []
    for table, filename in _LANDING_FILES.items():
        local_path = src / filename
        if not local_path.exists():
            raise FileNotFoundError(f"missing generated landing file: {local_path}")

        # io_paths gives s3://<bucket>/landing/<table>/ ; append the filename.
        prefix = io_paths.s3_layer_prefix("landing", table, ctx)
        s3_uri = f"{prefix}{filename}"
        bucket, key = _split_s3(s3_uri)

        print(f"uploading {local_path} -> {s3_uri}")
        s3.upload_file(str(local_path), bucket, key)
        written.append(s3_uri)

    return written


def _split_s3(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)`` (local helper)."""
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="upload_landing",
        description="Upload generated landing files to the S3 landing prefix (needs AWS creds).",
    )
    parser.add_argument("--src", default="./_local_out", help="Directory with generated files.")
    parser.add_argument("--env", default="dev", choices=["dev"], help="Environment (POC: dev).")
    parser.add_argument(
        "--account-id",
        required=True,
        help="AWS account id -- appended to the bucket base name (design S2.2).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    written = upload(args.src, account_id=args.account_id, env=args.env)
    print(f"uploaded {len(written)} landing files.")
    return 0


if __name__ == "__main__":  # pragma: no cover - requires AWS
    sys.exit(main())
