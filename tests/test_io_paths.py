"""Unit tests for src/common/io_paths.py -- exact path strings (incl. <table>)."""

from src.common import io_paths

# A fixed context so the asserted strings are deterministic regardless of the
# real account id. The default context is used where its placeholder is fine.
CTX = io_paths.IoContext(
    datalake_bucket="ocbc-poc-dev-datalake-123456789012",
    db_prefix="ocbc_poc_dev",
    catalog="glue_catalog",
)


def test_s3_layer_prefix():
    assert (
        io_paths.s3_layer_prefix("bronze", "customers", CTX)
        == "s3://ocbc-poc-dev-datalake-123456789012/bronze/customers/"
    )


def test_warehouse_uri():
    assert io_paths.warehouse_uri(CTX) == "s3://ocbc-poc-dev-datalake-123456789012/warehouse/"


def test_lineage_path_includes_table_segment():
    # The <table> segment is the final path component so per-table files never collide.
    assert (
        io_paths.lineage_path("RUN1", "bronze_to_silver", "customers", CTX)
        == "s3://ocbc-poc-dev-datalake-123456789012/lineage/RUN1/bronze_to_silver/customers.json"
    )


def test_quality_path_includes_table_segment():
    assert (
        io_paths.quality_path("RUN1", "bronze_to_silver", "transactions", CTX)
        == "s3://ocbc-poc-dev-datalake-123456789012/quality/RUN1/bronze_to_silver/transactions.json"
    )


def test_audit_json_path_includes_table_segment():
    assert (
        io_paths.audit_json_path("RUN1", "bronze_to_silver", "accounts", CTX)
        == "s3://ocbc-poc-dev-datalake-123456789012/audit/RUN1/bronze_to_silver/accounts.json"
    )


def test_glue_db():
    assert io_paths.glue_db("silver", CTX) == "ocbc_poc_dev_silver"
    assert io_paths.glue_db("audit", CTX) == "ocbc_poc_dev_audit"


def test_iceberg_table_format():
    assert (
        io_paths.iceberg_table("silver", "customers", CTX)
        == "glue_catalog.ocbc_poc_dev_silver.customers"
    )


def test_two_tables_do_not_collide():
    a = io_paths.lineage_path("R", "j", "customers", CTX)
    b = io_paths.lineage_path("R", "j", "accounts", CTX)
    assert a != b
