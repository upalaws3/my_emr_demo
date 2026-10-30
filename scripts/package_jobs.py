"""package_jobs.py -- build the src.zip shipped to EMR Serverless via pyFiles.

WHY THIS EXISTS
---------------
On EMR Serverless the Step Functions state machine sets each stage's
``EntryPoint`` to a single S3 object (e.g. ``s3://.../src/jobs/landing_to_bronze.py``).
EMR copies ONLY that one file into the job's working dir, so the job's imports
``from src.common ...`` / ``from data.generator ...`` and the runtime read of
``config/pipeline.yaml`` all fail with ModuleNotFoundError / file-not-found.

The standard fix is to ship the shared code as a zip through
``spark.submit.pyFiles``. Spark places the zip on ``sys.path`` on the driver and
every executor, so the packages inside become importable. We bundle THREE
top-level entries so every runtime import and the config read resolve:

  * ``src/``    -- the common modules + the job package,
  * ``data/``   -- data.generator.schema_defs (imported by the jobs),
  * ``config/`` -- pipeline.yaml (config_loader walks up to find it).

The zip is written next to the repo root and uploaded to
``s3://<datalake_bucket>/src.zip`` (the ``jobs_pyfiles`` local in Terraform).

USAGE
-----
    # build only (writes ./src.zip)
    python scripts/package_jobs.py

    # build and upload to the data-lake bucket (needs AWS creds)
    python scripts/package_jobs.py --upload --account-id 656111643793

``boto3`` is imported lazily so the build path works on a bare host.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import zipfile

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Top-level directories bundled into the zip. Paths inside the zip are kept
# relative to the repo root (so entries look like ``src/common/...``), which is
# exactly what ``import src.common`` and the config walk-up expect.
_BUNDLE_DIRS = ("src", "data", "config")

# Files we never want in the shipped artifact.
_SKIP_SUFFIXES = (".pyc",)
_SKIP_DIR_NAMES = {"__pycache__", ".pytest_cache"}


def _generate_config_json() -> pathlib.Path:
    """Render config/pipeline.yaml -> config/pipeline.json (dependency-free read).

    EMR Serverless does NOT ship PyYAML, so the jobs read a JSON copy of the
    config at runtime (config_loader prefers pipeline.json). We generate it here
    on the deploy host -- which DOES have PyYAML in its venv -- and bundle it in
    the zip. Keeping YAML as the human-edited source + JSON as the shipped
    artifact means the cluster never needs a third-party parser.
    """
    import json

    import yaml  # available on the deploy host (requirements.txt / venv)

    yaml_path = _REPO_ROOT / "config" / "pipeline.yaml"
    json_path = _REPO_ROOT / "config" / "pipeline.json"
    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    json_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    print(f"generated {json_path} from {yaml_path.name}")
    return json_path


def build_zip(out_path: pathlib.Path) -> pathlib.Path:
    """Zip src/ + data/ + config/ (repo-relative) into ``out_path``.

    Generates config/pipeline.json first so the shipped config is readable
    without PyYAML on the EMR Serverless runtime.
    """
    _generate_config_json()

    if out_path.exists():
        out_path.unlink()

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for top in _BUNDLE_DIRS:
            root = _REPO_ROOT / top
            if not root.exists():
                raise FileNotFoundError(f"expected directory not found: {root}")
            for file in sorted(root.rglob("*")):
                if file.is_dir():
                    continue
                if any(part in _SKIP_DIR_NAMES for part in file.parts):
                    continue
                if file.suffix in _SKIP_SUFFIXES:
                    continue
                arcname = file.relative_to(_REPO_ROOT).as_posix()
                zf.write(file, arcname)

    return out_path


def upload_zip(zip_path: pathlib.Path, account_id: str, env: str = "dev") -> str:
    """Upload the zip to s3://<project>-<env>-datalake-<account_id>/src.zip."""
    import boto3

    # Mirror the Terraform bucket-naming scheme without importing the full
    # config loader (keep this script dependency-light).
    sys.path.insert(0, str(_REPO_ROOT))
    from src.common import config_loader

    cfg = config_loader.load_config()
    bucket = f"{cfg.buckets['datalake']}-{account_id}"
    key = "src.zip"

    s3 = boto3.client("s3")
    print(f"uploading {zip_path} -> s3://{bucket}/{key}")
    s3.upload_file(str(zip_path), bucket, key)
    return f"s3://{bucket}/{key}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="package_jobs",
        description="Build (and optionally upload) the EMR Serverless src.zip.",
    )
    parser.add_argument(
        "--out",
        default=str(_REPO_ROOT / "src.zip"),
        help="Output zip path (default: <repo>/src.zip).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload the zip to the data-lake bucket after building (needs AWS creds).",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="AWS account id (required with --upload; part of the bucket name).",
    )
    parser.add_argument("--env", default="dev", choices=["dev"], help="Environment (POC: dev).")
    args = parser.parse_args(argv)

    out_path = build_zip(pathlib.Path(args.out))
    size_kb = out_path.stat().st_size / 1024
    print(f"built {out_path} ({size_kb:.1f} KiB) containing: {', '.join(_BUNDLE_DIRS)}")

    if args.upload:
        if not args.account_id:
            parser.error("--account-id is required with --upload")
        uri = upload_zip(out_path, account_id=args.account_id, env=args.env)
        print(f"uploaded: {uri}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
