"""audit.py -- run-level audit record + writer (design S5.5 / S7.3).

Each table processed by each job writes exactly ONE audit row (grain: one row
per table per job, design Finding 3) to the Iceberg ``pipeline_runs`` table and
a durable JSON copy to S3. This is the observability spine: who ran what, when,
how many rows, did DQ pass.

Key naming note: the per-table field is ``table_name`` (NEVER ``table`` -- a
reserved word in Spark SQL / Athena / Trino). The post-write fields are Optional
(default ``None``) so the audit write on the DQ-failure path -- which runs
BEFORE the silver write -- never crashes on a missing value (design Finding 5).

``pyspark`` is imported lazily inside :func:`write_audit`.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

# The single canonical status enumeration (design S5.5). No other value is emitted.
STATUS_SUCCESS = "SUCCESS"  # table completed and passed its gate
STATUS_DQ_FAILED = "DQ_FAILED"  # a critical DQ expectation tripped the gate
STATUS_FAILED = "FAILED"  # any other fatal error (read/write/config)
STATUSES = (STATUS_SUCCESS, STATUS_DQ_FAILED, STATUS_FAILED)


@dataclass
class AuditRecord:
    """One audit row for one table in one job (design S7.3).

    Required fields are always populatable by the time any audit row is written
    (``input_rows`` is set as soon as the source read succeeds). The post-write
    fields below are Optional (default ``None``) so the DQ-failure path -- which
    writes its row before the silver write and before lineage emit -- never
    crashes on a missing value.
    """

    # --- always present ---
    run_id: str
    job: str
    layer_from: str
    layer_to: str
    table_name: str  # NOTE: table_name, never `table` (reserved word)
    status: str  # one of STATUSES
    input_rows: int
    dq_passed: bool
    dq_critical_failed: int
    started_at: str
    ended_at: str
    duration_ms: int

    # --- optional post-write fields (None on the DQ-failure / pre-write paths) ---
    output_rows: int | None = None
    rejected_rows: int | None = None
    lineage_path: str | None = None
    quality_path: str | None = None
    emr_application_id: str | None = None
    spark_app_id: str | None = None

    def __post_init__(self) -> None:
        # Fail fast on an out-of-enum status so a typo never reaches the table.
        if self.status not in STATUSES:
            raise ValueError(f"invalid audit status {self.status!r}; expected one of {STATUSES}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the design S7.3 JSON shape (key order matches the doc)."""
        # dataclasses.asdict preserves declaration order; that already matches
        # S7.3 closely enough for the durable JSON copy.
        return asdict(self)


# Iceberg DDL for the audit table (design S7.3). Unpartitioned (Finding 6): at
# POC scale it holds a handful of rows per run and partition-transform semantics
# differ across Spark/Athena, so the baseline keeps it simple.
_PIPELINE_RUNS_DDL_COLUMNS = (
    "run_id string, job string, layer_from string, layer_to string, "
    "table_name string, status string, input_rows bigint, output_rows bigint, "
    "rejected_rows bigint, dq_passed boolean, dq_critical_failed int, "
    "started_at timestamp, ended_at timestamp, duration_ms bigint, "
    "lineage_path string, quality_path string, emr_application_id string, "
    "spark_app_id string"
)


def write_audit(
    spark,
    record: AuditRecord,
    catalog: str,
    database: str,
    table: str = "pipeline_runs",
    s3_json_dir: str | None = None,
    ctx=None,
) -> None:
    """Append exactly ONE row for one table to the audit Iceberg table + JSON copy.

    Steps:
      1. ``CREATE TABLE IF NOT EXISTS`` the unpartitioned Iceberg ``pipeline_runs``.
      2. Append the single ``record`` row.
      3. Write a durable JSON copy to ``io_paths.audit_json_path(run_id, job,
         table_name)`` (S3 via the Spark Hadoop FS when ``s3_json_dir`` is an
         s3 uri, else to a local directory).

    ``pyspark`` is imported lazily so this module stays import-safe.
    """
    from pyspark.sql import Row

    fq_table = f"{catalog}.{database}.{table}"

    # 1. create-if-not-exists (unpartitioned).
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq_table} ({_PIPELINE_RUNS_DDL_COLUMNS}) USING iceberg")

    # 2. append exactly one row (the record for this table).
    row_df = spark.createDataFrame([Row(**record.to_dict())])
    row_df.writeTo(fq_table).append()

    # 3. durable JSON mirror (per-table path -- design Finding 1/3).
    from . import io_paths

    json_uri = io_paths.audit_json_path(record.run_id, record.job, record.table_name, ctx)
    body = json.dumps(record.to_dict(), indent=2, default=str)
    _write_json(spark, json_uri, body, s3_json_dir)


def _write_json(spark, json_uri: str, body: str, s3_json_dir: str | None) -> None:
    """Write the JSON copy via the Spark Hadoop filesystem (works for S3 + local).

    Using the Hadoop FS keeps us from needing boto3 inside a Spark job (the
    cluster already has the S3 connector). ``s3_json_dir``, when given, overrides
    the directory while keeping the ``<table>.json`` filename.
    """
    # Allow an explicit local/alternate directory override for tests/dev.
    if s3_json_dir is not None:
        filename = json_uri.rsplit("/", 1)[-1]
        json_uri = f"{s3_json_dir.rstrip('/')}/{filename}"

    jvm = spark._jvm
    jsc = spark._jsc
    hadoop_conf = jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(json_uri)
    fs = path.getFileSystem(hadoop_conf)
    out = fs.create(path, True)  # overwrite=True
    try:
        out.write(body.encode("utf-8"))
    finally:
        out.close()
