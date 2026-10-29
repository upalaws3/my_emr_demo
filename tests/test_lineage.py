"""Unit tests for src/common/lineage.py (builds docs from dicts, no Spark/AWS)."""

import json
import pathlib

from src.common import lineage


def test_builds_s7_1_shape():
    b = lineage.start_lineage("RUN1", "bronze_to_silver", "bronze", "silver", "customers")
    b.add_input(
        "s3://.../bronze/customers/",
        "parquet",
        row_count=100,
        glue_table="ocbc_poc_dev_bronze.customers",
    )
    b.add_output(
        "glue_catalog.ocbc_poc_dev_silver.customers",
        "iceberg",
        row_count=100,
        glue_table="ocbc_poc_dev_silver.customers",
    )
    b.add_column_mapping("email", ["email"], "trim")
    b.set_metrics(input_rows=100, output_rows=100, rejected_rows=0)

    doc = b.to_dict()
    assert doc["schema_version"] == "1.0"
    assert doc["run_id"] == "RUN1"
    assert doc["job"] == "bronze_to_silver"
    assert doc["table"] == "customers"
    assert doc["layer_from"] == "bronze"
    assert doc["layer_to"] == "silver"
    assert doc["status"] == "SUCCESS"
    assert doc["inputs"][0]["glue_table"] == "ocbc_poc_dev_bronze.customers"
    assert doc["outputs"][0]["format"] == "iceberg"
    assert doc["column_mappings"][0] == {
        "output": "email",
        "inputs": ["email"],
        "transform": "trim",
    }
    assert doc["metrics"] == {"input_rows": 100, "output_rows": 100, "rejected_rows": 0}
    # All required top-level keys present.
    for key in (
        "started_at",
        "ended_at",
        "inputs",
        "outputs",
        "column_mappings",
        "metrics",
    ):
        assert key in doc


def test_landing_input_omits_glue_table_but_cataloged_carries_it():
    b = lineage.start_lineage("RUN1", "landing_to_bronze", "landing", "bronze", "customers")
    # Landing input is uncataloged -> glue_table omitted (None default).
    b.add_input("s3://.../landing/customers/", "csv", row_count=100)
    # Bronze output is cataloged -> glue_table present.
    b.add_output(
        "s3://.../bronze/customers/",
        "parquet",
        row_count=100,
        glue_table="ocbc_poc_dev_bronze.customers",
    )
    doc = b.to_dict()
    assert "glue_table" not in doc["inputs"][0]
    assert doc["outputs"][0]["glue_table"] == "ocbc_poc_dev_bronze.customers"


def test_emit_writes_per_table_file_and_no_collision(tmp_path):
    out_dir = tmp_path / "lineage"

    b1 = lineage.start_lineage("RUN1", "bronze_to_silver", "bronze", "silver", "customers")
    b1.add_input("s3://.../bronze/customers/", "parquet", row_count=100)
    path1 = lineage.emit(b1, local_dir=str(out_dir))

    b2 = lineage.start_lineage("RUN1", "bronze_to_silver", "bronze", "silver", "accounts")
    b2.add_input("s3://.../bronze/accounts/", "parquet", row_count=150)
    path2 = lineage.emit(b2, local_dir=str(out_dir))

    assert pathlib.Path(path1).name == "customers.json"
    assert pathlib.Path(path2).name == "accounts.json"
    assert path1 != path2  # two tables never collide
    # Written file is valid JSON matching the builder.
    doc = json.loads(pathlib.Path(path1).read_text(encoding="utf-8"))
    assert doc["table"] == "customers"
