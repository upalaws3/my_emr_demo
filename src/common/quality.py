"""quality.py -- data-quality expectations, evaluation, and the FAIL gate.

Design S5.4 / S6.2a / S7.2. Expectations are declared as small dataclasses
(usually built from ``config/pipeline.yaml`` DQ rules), evaluated in Spark with
count-based checks (no row-by-row UDFs), serialized to a per-table quality JSON
(S7.2), and finally gated: a CRITICAL failure trips :func:`enforce_gate`.

The call sequence is a HARD CONTRACT owned by the caller (bronze_to_silver,
S6.2a): ``run_expectations -> emit_quality (ALWAYS) -> enforce_gate``.
``enforce_gate`` performs NO I/O so it stays trivially unit-testable.

``pyspark`` is imported lazily inside :func:`run_expectations` only, so this
module is import-safe on a host without Spark.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

# JSON schema version stamped into every quality document (design S7.2).
SCHEMA_VERSION = "1.0"

# Allowed severities. Only "critical" failures trip the gate; "warn" is recorded.
SEVERITY_CRITICAL = "critical"
SEVERITY_WARN = "warn"


class QualityGateError(Exception):
    """Raised by :func:`enforce_gate` when a critical expectation failed.

    The non-zero exit this drives is what Step Functions' ``Catch[States.ALL]``
    traps to route to the shared PipelineFailed Fail state (design S8).
    """


@dataclass
class Expectation:
    """One declarative data-quality check.

    Attributes
    ----------
    name:
        Stable identifier (also the key in the quality JSON).
    kind:
        One of ``not_null``, ``unique``, ``in_set``, ``range``, ``referential``,
        ``row_count``.
    columns:
        The column(s) the check applies to (empty for ``row_count``).
    params:
        Extra parameters keyed by kind: ``allowed`` (in_set), ``min``/``max``
        (range, row_count), ``ref_table``/``ref_col`` (referential).
    severity:
        ``"critical"`` or ``"warn"``.
    """

    name: str
    kind: str
    columns: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    severity: str = SEVERITY_CRITICAL

    @property
    def ref_table_name(self) -> str | None:
        """Parent table NAME for a referential expectation (a string, never a DF)."""
        return self.params.get("ref_table")

    @property
    def ref_col(self) -> str | None:
        """Parent column for a referential expectation."""
        return self.params.get("ref_col")


# -- Builders (design S5.4) ------------------------------------------------
# Each returns an Expectation. Severity defaults to critical for structural
# checks and warn for softer ones, matching the config defaults; callers built
# from config pass severity explicitly.


def expect_not_null(col: str, severity: str = SEVERITY_CRITICAL) -> Expectation:
    """No NULLs allowed in ``col``."""
    return Expectation(f"{col}_not_null", "not_null", [col], {}, severity)


def expect_unique(cols: list[str], severity: str = SEVERITY_CRITICAL) -> Expectation:
    """The combination of ``cols`` must be unique (no duplicate keys)."""
    name = "_".join(cols) + "_unique"
    return Expectation(name, "unique", list(cols), {}, severity)


def expect_in_set(col: str, allowed: list[Any], severity: str = SEVERITY_WARN) -> Expectation:
    """Every value of ``col`` must be in ``allowed``."""
    return Expectation(f"{col}_in_set", "in_set", [col], {"allowed": list(allowed)}, severity)


def expect_range(
    col: str, min: Any = None, max: Any = None, severity: str = SEVERITY_WARN
) -> Expectation:
    """``col`` must lie within ``[min, max]`` (either bound optional)."""
    return Expectation(f"{col}_range", "range", [col], {"min": min, "max": max}, severity)


def expect_referential(
    col: str, ref_table_name: str, ref_col: str, severity: str = SEVERITY_CRITICAL
) -> Expectation:
    """``col`` must exist in parent ``ref_table_name.ref_col`` (FK integrity).

    Stores only the parent table NAME string (design Finding 5); the caller
    supplies the actual DataFrame via ``ref_dfs[ref_table_name]``.
    """
    return Expectation(
        f"{col}_referential",
        "referential",
        [col],
        {"ref_table": ref_table_name, "ref_col": ref_col},
        severity,
    )


def expect_row_count(
    min: int, max: int | None = None, severity: str = SEVERITY_WARN
) -> Expectation:
    """The table row count must be ``>= min`` (and ``<= max`` when given)."""
    return Expectation("row_count_min", "row_count", [], {"min": min, "max": max}, severity)


@dataclass
class QualityResult:
    """Outcome of evaluating a table's expectations (design S7.2).

    ``passed`` is True iff NO critical expectation failed; warn failures never
    flip ``passed``.
    """

    run_id: str
    job: str
    table: str
    evaluated_at: str
    critical_failed: int
    warn_failed: int
    expectations: list[dict[str, Any]]

    @property
    def passed(self) -> bool:
        """True iff no critical expectation failed (warn failures are tolerated)."""
        return self.critical_failed == 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the design S7.2 JSON shape (as a dict)."""
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "job": self.job,
            "table": self.table,
            "evaluated_at": self.evaluated_at,
            "passed": self.passed,
            "critical_failed": self.critical_failed,
            "warn_failed": self.warn_failed,
            "expectations": self.expectations,
        }


def run_expectations(
    df,
    expectations: list[Expectation],
    run_id: str,
    job: str,
    table: str,
    ref_dfs: dict[str, Any] | None = None,
) -> QualityResult:
    """Evaluate all ``expectations`` against ``df`` and build a QualityResult.

    Checks are count-based (Spark aggregations, no row-by-row UDFs). Referential
    checks resolve the parent via ``ref_dfs[exp.ref_table_name]``; a MISSING key
    is a HARD failure (a child validated before its parent exists is never
    silently passed -- design S5.4/S6.2).

    ``pyspark`` is imported lazily (only needed here).
    """
    from pyspark.sql import functions as F  # noqa: F401  (lazy; used below)

    ref_dfs = ref_dfs or {}
    results: list[dict[str, Any]] = []
    critical_failed = 0
    warn_failed = 0

    total_rows = df.count()

    for exp in expectations:
        observed: dict[str, Any] = {}

        if exp.kind == "not_null":
            col = exp.columns[0]
            nulls = df.filter(F.col(col).isNull()).count()
            observed = {"null_count": nulls}
            passed = nulls == 0

        elif exp.kind == "unique":
            distinct_keys = df.select(*exp.columns).distinct().count()
            duplicate_count = total_rows - distinct_keys
            observed = {"duplicate_count": duplicate_count}
            passed = duplicate_count == 0

        elif exp.kind == "in_set":
            col = exp.columns[0]
            allowed = exp.params.get("allowed", [])
            violations = df.filter(F.col(col).isNotNull() & ~F.col(col).isin(allowed)).count()
            observed = {"out_of_set_count": violations}
            passed = violations == 0

        elif exp.kind == "range":
            col = exp.columns[0]
            lo = exp.params.get("min")
            hi = exp.params.get("max")
            cond = F.col(col).isNotNull()
            out = df.filter(cond)
            violations = 0
            if lo is not None:
                violations += out.filter(F.col(col) < F.lit(lo)).count()
            if hi is not None:
                violations += out.filter(F.col(col) > F.lit(hi)).count()
            observed = {"out_of_range_count": violations}
            passed = violations == 0

        elif exp.kind == "referential":
            name = exp.ref_table_name
            if name not in ref_dfs:
                # Hard failure: never silently pass a missing-parent referential.
                raise QualityGateError(
                    f"event=dq_fail referential parent {name!r} missing from ref_dfs "
                    f"for expectation {exp.name!r} on table {table!r}"
                )
            parent = ref_dfs[name]
            child_col = exp.columns[0]
            ref_col = exp.ref_col
            # Left-anti join: child rows whose FK has no matching parent key.
            child_keys = df.filter(F.col(child_col).isNotNull()).select(
                F.col(child_col).alias("_fk")
            )
            parent_keys = parent.select(F.col(ref_col).alias("_fk")).distinct()
            orphans = child_keys.join(parent_keys, on="_fk", how="left_anti").count()
            observed = {"orphans": orphans}
            passed = orphans == 0

        elif exp.kind == "row_count":
            lo = exp.params.get("min", 1)
            hi = exp.params.get("max")
            passed = total_rows >= lo and (hi is None or total_rows <= hi)
            observed = {"row_count": total_rows}

        else:  # pragma: no cover - guards against a config/typo mismatch
            raise ValueError(f"unknown expectation kind {exp.kind!r} ({exp.name})")

        if not passed:
            if exp.severity == SEVERITY_CRITICAL:
                critical_failed += 1
            else:
                warn_failed += 1

        results.append(
            {
                "name": exp.name,
                "kind": exp.kind,
                "columns": exp.columns,
                "severity": exp.severity,
                "passed": passed,
                "observed": observed,
                "params": exp.params,
            }
        )

    return QualityResult(
        run_id=run_id,
        job=job,
        table=table,
        evaluated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        critical_failed=critical_failed,
        warn_failed=warn_failed,
        expectations=results,
    )


def emit_quality(
    result: QualityResult,
    s3_client: Any = None,
    local_dir: str | None = None,
    ctx=None,
) -> str:
    """Write ``result`` as JSON to the per-table quality path; return the path.

    Destination is ``io_paths.quality_path(result.run_id, result.job,
    result.table)``. S3 via boto3 when ``s3_client`` is given, else under
    ``local_dir`` keeping the ``<table>.json`` name so files never collide.
    """
    from . import io_paths

    s3_uri = io_paths.quality_path(result.run_id, result.job, result.table, ctx)
    body = json.dumps(result.to_dict(), indent=2, default=str)

    if s3_client is not None:
        from .lineage import _split_s3

        bucket, key = _split_s3(s3_uri)
        s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
        return s3_uri

    import pathlib

    base = pathlib.Path(local_dir or ".")
    base.mkdir(parents=True, exist_ok=True)
    out = base / f"{result.table}.json"
    out.write_text(body, encoding="utf-8")
    return str(out)


def enforce_gate(result: QualityResult) -> None:
    """Raise :class:`QualityGateError` iff a CRITICAL expectation failed.

    Performs NO I/O (design S5.4): it takes only ``result`` so it is trivially
    unit-testable. The "quality JSON is always persisted before any raise" and
    "DQ_FAILED audit row written before re-raise" invariants are caller-owned
    (design S6.2a).
    """
    if not result.passed:
        raise QualityGateError(
            f"event=dq_fail critical DQ failure on table {result.table!r}: "
            f"{result.critical_failed} critical expectation(s) failed"
        )
