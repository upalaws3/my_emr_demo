"""run_local_checks.py -- the local verification gate (design S13).

Orchestrates the layered checks that CAN run on a bare Windows host (no AWS, no
Java, no pyspark) and SKIPS the tool-dependent ones instead of failing. Prints a
summary table and exits non-zero ONLY if a MANDATORY step failed.

Steps (design S13):
  1. Byte-compile all Python + py_compile each job entry point     (MANDATORY)
  2. Import-safe check of every src/common module (no Spark built) (MANDATORY)
  3. Run the generator + assert counts in range + PII populated     (MANDATORY)
  4. pytest tests/                                                  (MANDATORY)
  5. terraform init -backend=false / validate / fmt -check          (skip-if-absent)
  6. local Spark+Iceberg smoke (scripts/smoke_local_spark.py)       (skip-if-absent)

Stdlib only (plus whatever pytest/subprocess call). This is the canonical
one-command local gate: ``python scripts/run_local_checks.py``.
"""

from __future__ import annotations

import importlib
import pathlib
import shutil
import subprocess
import sys

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Status tokens for the summary.
_PASS = "PASS"
_FAIL = "FAIL"
_SKIP = "SKIPPED"

# The four job entry points py_compile must cover individually (design S13.1).
_JOB_ENTRYPOINTS = (
    "src/jobs/landing_to_bronze.py",
    "src/jobs/bronze_to_silver.py",
    "src/jobs/silver_to_canonical.py",
    "src/jobs/canonical_to_gold.py",
)

# Every src/common module + its expected public attribute(s) for step 2.
_COMMON_PUBLIC = {
    "src.common.config_loader": ["load_config", "Config", "ConfigError"],
    "src.common.io_paths": ["s3_layer_prefix", "iceberg_table", "glue_db", "IoContext"],
    "src.common.logging_util": ["get_logger", "log_event", "timed"],
    "src.common.masking": ["mask_name", "tokenize_id", "apply_masking", "pii_columns_for"],
    "src.common.lineage": ["start_lineage", "emit", "register_glue_table"],
    "src.common.quality": ["run_expectations", "emit_quality", "enforce_gate", "QualityGateError"],
    "src.common.audit": ["AuditRecord", "write_audit", "STATUSES"],
    "src.common.spark_session": ["build_spark"],
}

_LOCAL_OUT = _REPO_ROOT / "_local_out"


def _run(cmd: list[str]) -> tuple[int, str]:
    """Run a subprocess from the repo root; return (returncode, combined output)."""
    proc = subprocess.run(
        cmd, cwd=str(_REPO_ROOT), capture_output=True, text=True
    )
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def step1_compileall() -> tuple[str, str]:
    """Byte-compile src/data/scripts + py_compile each job entry point."""
    rc, out = _run([sys.executable, "-m", "compileall", "-q", "src", "data", "scripts"])
    if rc != 0:
        return _FAIL, out
    for entry in _JOB_ENTRYPOINTS:
        rc, out = _run([sys.executable, "-m", "py_compile", entry])
        if rc != 0:
            return _FAIL, f"py_compile {entry} failed:\n{out}"
    return _PASS, "compileall + per-job py_compile OK"


def step2_import_safe() -> tuple[str, str]:
    """Import every src/common module and assert its public functions exist.

    No Spark session is constructed (only module import), so this passes on the
    bare host.
    """
    for module_name, publics in _COMMON_PUBLIC.items():
        try:
            mod = importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            return _FAIL, f"import {module_name} failed: {exc}"
        for attr in publics:
            if not hasattr(mod, attr):
                return _FAIL, f"{module_name} missing public attr {attr!r}"
    return _PASS, "all src/common modules import-safe with public API present"


def step3_generator() -> tuple[str, str]:
    """Run the generator and assert counts in configured range + PII populated."""
    from data.generator import schema_defs
    from data.generator.generate import generate, read_row_count_ranges

    ranges = read_row_count_ranges()
    counts = generate(str(_LOCAL_OUT), inject_dq_failure=False, seed=42)

    # Assert each produced count falls inside its configured [min, max].
    for table, (lo, hi) in ranges.items():
        n = counts[table]
        if not (lo <= n <= hi):
            return _FAIL, f"{table} count {n} outside configured range [{lo},{hi}]"

    # Assert PII columns are present and populated in the generated files.
    import csv
    import json

    # customers.csv PII columns.
    cust_pii = schema_defs.pii_columns("customers")
    with (_LOCAL_OUT / "customers.csv").open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        first = next(reader, None)
    if first is None:
        return _FAIL, "customers.csv has no rows"
    for col in cust_pii:
        if not first.get(col):
            return _FAIL, f"customers PII column {col!r} empty in first row"

    # cards.csv PII columns.
    card_pii = schema_defs.pii_columns("cards")
    with (_LOCAL_OUT / "cards.csv").open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        first_card = next(reader, None)
    if first_card is None:
        return _FAIL, "cards.csv has no rows"
    for col in card_pii:
        if not first_card.get(col):
            return _FAIL, f"cards PII column {col!r} empty in first row"

    # transactions.json parses and is non-empty.
    txns = json.loads((_LOCAL_OUT / "transactions.json").read_text(encoding="utf-8"))
    if not txns:
        return _FAIL, "transactions.json has no rows"

    detail = ", ".join(f"{t}={counts[t]}" for t in schema_defs.TABLE_ORDER)
    return _PASS, f"generator counts in range + PII populated ({detail})"


def step4_pytest() -> tuple[str, str]:
    """Run the pure-Python unit test suite."""
    rc, out = _run([sys.executable, "-m", "pytest", "tests/", "-q"])
    tail = "\n".join(out.strip().splitlines()[-6:])
    return (_PASS if rc == 0 else _FAIL), tail


def step5_terraform() -> tuple[str, str]:
    """Terraform init/validate/fmt -- skip if terraform is not on PATH."""
    if shutil.which("terraform") is None:
        return _SKIP, "terraform not installed"
    tf_dir = "infra/terraform"
    if not (_REPO_ROOT / tf_dir).exists():
        return _SKIP, "infra/terraform not present (authored in a later feature)"
    for cmd in (
        ["terraform", f"-chdir={tf_dir}", "init", "-backend=false"],
        ["terraform", f"-chdir={tf_dir}", "validate"],
        ["terraform", f"-chdir={tf_dir}", "fmt", "-check"],
    ):
        rc, out = _run(cmd)
        if rc != 0:
            return _FAIL, f"{' '.join(cmd)} failed:\n{out}"
    return _PASS, "terraform init/validate/fmt OK"


def step6_spark_smoke() -> tuple[str, str]:
    """Run the optional local Spark+Iceberg smoke -- skip if pyspark/java absent."""
    rc, out = _run([sys.executable, "scripts/smoke_local_spark.py"])
    last = out.strip().splitlines()[-1] if out.strip() else ""
    if "SKIPPED" in out:
        return _SKIP, "pyspark and/or java not available"
    return (_PASS if rc == 0 else _FAIL), last


def main() -> int:
    # (name, fn, mandatory)
    steps = [
        ("1. compileall + py_compile", step1_compileall, True),
        ("2. import-safe common", step2_import_safe, True),
        ("3. generator in-range + PII", step3_generator, True),
        ("4. pytest", step4_pytest, True),
        ("5. terraform", step5_terraform, False),
        ("6. spark smoke", step6_spark_smoke, False),
    ]

    results = []
    mandatory_failed = False
    for name, fn, mandatory in steps:
        try:
            status, detail = fn()
        except Exception as exc:  # noqa: BLE001 - a crashing step is a failure
            status, detail = _FAIL, f"exception: {exc}"
        if status == _FAIL and mandatory:
            mandatory_failed = True
        results.append((name, status, detail))

    # Clean up the generator output so it is never committed (gitignored anyway).
    shutil.rmtree(_LOCAL_OUT, ignore_errors=True)

    # --- summary table --------------------------------------------------------
    print("\n==================== local checks summary ====================")
    width = max(len(n) for n, _, _ in results)
    for name, status, detail in results:
        print(f"  {name.ljust(width)}  {status:<8} {detail}")
    print("==============================================================")
    print("RESULT:", "FAILED" if mandatory_failed else "OK (mandatory steps passed)")

    return 1 if mandatory_failed else 0


if __name__ == "__main__":
    sys.exit(main())
