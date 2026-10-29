"""Unit tests for src/common/audit.py (dataclass serialization, no Spark)."""

import json

import pytest

from src.common import audit


def test_audit_record_serializes_with_table_name_key():
    rec = audit.AuditRecord(
        run_id="RUN1",
        job="bronze_to_silver",
        layer_from="bronze",
        layer_to="silver",
        table_name="transactions",
        status=audit.STATUS_SUCCESS,
        input_rows=800,
        output_rows=798,
        rejected_rows=2,
        dq_passed=True,
        dq_critical_failed=0,
        started_at="2025-01-01T10:00:00Z",
        ended_at="2025-01-01T10:00:42Z",
        duration_ms=42000,
        lineage_path="s3://.../lineage/RUN1/bronze_to_silver/transactions.json",
        quality_path="s3://.../quality/RUN1/bronze_to_silver/transactions.json",
        emr_application_id="00f",
        spark_app_id="app-1",
    )
    doc = rec.to_dict()
    # The key is table_name, NEVER table (reserved word).
    assert "table_name" in doc
    assert "table" not in doc
    assert doc["table_name"] == "transactions"
    assert doc["status"] == "SUCCESS"
    json.loads(json.dumps(doc))  # round-trips cleanly


def test_dq_failure_shape_leaves_optional_fields_none():
    # On the DQ-failure path the silver write never happens, so post-write fields
    # stay None and the record must still construct without error.
    rec = audit.AuditRecord(
        run_id="RUN1",
        job="bronze_to_silver",
        layer_from="bronze",
        layer_to="silver",
        table_name="transactions",
        status=audit.STATUS_DQ_FAILED,
        input_rows=801,  # populated: the bronze read succeeded
        dq_passed=False,
        dq_critical_failed=1,
        started_at="2025-01-01T10:00:00Z",
        ended_at="2025-01-01T10:00:10Z",
        duration_ms=10000,
        quality_path="s3://.../quality/RUN1/bronze_to_silver/transactions.json",
    )
    doc = rec.to_dict()
    assert doc["status"] == "DQ_FAILED"
    assert doc["output_rows"] is None
    assert doc["rejected_rows"] is None
    assert doc["lineage_path"] is None
    assert doc["emr_application_id"] is None
    assert doc["spark_app_id"] is None
    assert doc["input_rows"] == 801


def test_invalid_status_rejected():
    with pytest.raises(ValueError):
        audit.AuditRecord(
            run_id="R",
            job="j",
            layer_from="bronze",
            layer_to="silver",
            table_name="t",
            status="BOGUS",
            input_rows=1,
            dq_passed=True,
            dq_critical_failed=0,
            started_at="a",
            ended_at="b",
            duration_ms=1,
        )
