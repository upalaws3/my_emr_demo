"""Unit tests for src/common/quality.py.

enforce_gate and QualityResult.passed are exercised without Spark (run_expectations
needs Spark, so we build QualityResult directly from a stub to assert the S7.2 shape).
"""

import json

import pytest

from src.common import quality


def _result(critical_failed, warn_failed=0, table="transactions"):
    """Build a QualityResult directly (bypasses Spark) for gate/shape tests."""
    expectations = [
        {
            "name": "transaction_id_unique",
            "kind": "unique",
            "columns": ["transaction_id"],
            "severity": "critical",
            "passed": critical_failed == 0,
            "observed": {"duplicate_count": critical_failed},
            "params": {},
        },
        {
            "name": "account_id_referential",
            "kind": "referential",
            "columns": ["account_id"],
            "severity": "critical",
            "passed": True,
            "observed": {"orphans": 0},
            "params": {"ref_table": "accounts", "ref_col": "account_id"},
        },
    ]
    return quality.QualityResult(
        run_id="RUN1",
        job="bronze_to_silver",
        table=table,
        evaluated_at="2025-01-01T10:00:30Z",
        critical_failed=critical_failed,
        warn_failed=warn_failed,
        expectations=expectations,
    )


def test_enforce_gate_raises_on_critical_failure():
    with pytest.raises(quality.QualityGateError) as exc:
        quality.enforce_gate(_result(critical_failed=1))
    assert "dq_fail" in str(exc.value)


def test_enforce_gate_no_raise_when_only_warn_failed():
    # A warn-only failure must NOT trip the gate.
    quality.enforce_gate(_result(critical_failed=0, warn_failed=2))  # no exception


def test_quality_result_passed_logic():
    assert _result(critical_failed=0).passed is True
    assert _result(critical_failed=0, warn_failed=5).passed is True  # warn never flips passed
    assert _result(critical_failed=1).passed is False


def test_s7_2_json_shape():
    doc = _result(critical_failed=1).to_dict()
    assert doc["schema_version"] == "1.0"
    for key in (
        "run_id",
        "job",
        "table",
        "evaluated_at",
        "passed",
        "critical_failed",
        "warn_failed",
        "expectations",
    ):
        assert key in doc
    assert doc["passed"] is False
    assert doc["critical_failed"] == 1
    exp0 = doc["expectations"][0]
    for key in ("name", "kind", "columns", "severity", "passed", "observed", "params"):
        assert key in exp0
    # Round-trips through json cleanly.
    json.loads(json.dumps(doc))


def test_emit_quality_writes_per_table_file(tmp_path):
    out_dir = tmp_path / "quality"
    p1 = quality.emit_quality(_result(0, table="customers"), local_dir=str(out_dir))
    p2 = quality.emit_quality(_result(0, table="accounts"), local_dir=str(out_dir))
    assert p1.endswith("customers.json")
    assert p2.endswith("accounts.json")
    assert p1 != p2


def test_expect_builders():
    # Referential stores the parent NAME string, never a DataFrame.
    exp = quality.expect_referential("customer_id", "customers", "customer_id")
    assert exp.ref_table_name == "customers"
    assert exp.ref_col == "customer_id"
    assert exp.kind == "referential"
    uniq = quality.expect_unique(["transaction_id"])
    assert uniq.kind == "unique"
    assert uniq.severity == "critical"
    rc = quality.expect_row_count(1, severity="critical")
    assert rc.kind == "row_count"
    assert rc.params["min"] == 1
