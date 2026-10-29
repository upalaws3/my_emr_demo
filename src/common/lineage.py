"""lineage.py -- lightweight custom JSON lineage (design S5.3 / S7.1).

Instead of an external lineage service (OpenLineage/Marquez are out of scope for
the baseline), each job builds a small per-table lineage document and writes it
to ``lineage/<run_id>/<job>/<table>.json`` -- one file per table so a 4-table
run produces four files that never collide (design Finding 1).

``register_glue_table`` additionally registers a dataset in the Glue Data
Catalog so Athena can read it. boto3 is imported LAZILY so this module is
import-safe on a host without boto3.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from . import io_paths

# JSON schema version stamped into every lineage document (design S7.1).
SCHEMA_VERSION = "1.0"


def _now_iso() -> str:
    """UTC ISO-8601 timestamp with a trailing Z."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class LineageBuilder:
    """Accumulates one table's lineage, then serialized by :func:`emit`.

    The builder is PER TABLE: it carries ``table`` so ``emit`` derives its own
    per-table path (design Finding 1). ``layer_from``/``layer_to``/``table`` are
    set at construction and never inferred later.
    """

    run_id: str
    job: str
    layer_from: str
    layer_to: str
    table: str
    started_at: str = field(default_factory=_now_iso)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    column_mappings: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    status: str = "SUCCESS"

    def _dataset(
        self,
        dataset: str,
        fmt: str,
        row_count: int | None,
        columns: list[str] | None,
        glue_table: str | None,
    ) -> dict[str, Any]:
        """Build one input/output entry.

        ``glue_table`` is OMITTED entirely for an uncataloged dataset (the
        landing input), per design Finding 4 -- a reader treats a missing key as
        "uncataloged", never as an error.
        """
        entry: dict[str, Any] = {"dataset": dataset, "format": fmt}
        if row_count is not None:
            entry["row_count"] = row_count
        if columns is not None:
            entry["columns"] = columns
        if glue_table is not None:
            entry["glue_table"] = glue_table
        return entry

    def add_input(
        self,
        dataset: str,
        fmt: str,
        row_count: int | None = None,
        columns: list[str] | None = None,
        glue_table: str | None = None,
    ) -> "LineageBuilder":
        """Record an input dataset. Pass ``glue_table=None`` for landing (uncataloged)."""
        self.inputs.append(self._dataset(dataset, fmt, row_count, columns, glue_table))
        return self

    def add_output(
        self,
        dataset: str,
        fmt: str,
        row_count: int | None = None,
        columns: list[str] | None = None,
        glue_table: str | None = None,
    ) -> "LineageBuilder":
        """Record an output dataset (cataloged datasets carry ``glue_table``)."""
        self.outputs.append(self._dataset(dataset, fmt, row_count, columns, glue_table))
        return self

    def add_column_mapping(
        self, output_col: str, inputs: list[str], transform: str
    ) -> "LineageBuilder":
        """Record how one output column derives from input columns.

        ``transform`` is a short tag such as ``"trim"``, ``"mask:email"`` or
        ``"agg:sum"`` so masking/aggregation is visible in lineage.
        """
        self.column_mappings.append(
            {"output": output_col, "inputs": list(inputs), "transform": transform}
        )
        return self

    def set_metrics(self, **metrics: Any) -> "LineageBuilder":
        """Attach run metrics (e.g. ``input_rows``, ``output_rows``, ``rejected_rows``)."""
        self.metrics.update(metrics)
        return self

    def to_dict(self, ended_at: str | None = None) -> dict[str, Any]:
        """Serialize to the design S7.1 JSON shape (as a dict)."""
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "job": self.job,
            "table": self.table,
            "layer_from": self.layer_from,
            "layer_to": self.layer_to,
            "started_at": self.started_at,
            "ended_at": ended_at or _now_iso(),
            "status": self.status,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "column_mappings": self.column_mappings,
            "metrics": self.metrics,
        }


def start_lineage(
    run_id: str, job: str, layer_from: str, layer_to: str, table: str
) -> LineageBuilder:
    """Create a per-table :class:`LineageBuilder` (design S5.3)."""
    return LineageBuilder(
        run_id=run_id, job=job, layer_from=layer_from, layer_to=layer_to, table=table
    )


def emit(
    builder: LineageBuilder,
    s3_client: Any = None,
    local_dir: str | None = None,
    ctx: io_paths.IoContext | None = None,
) -> str:
    """Serialize ``builder`` to JSON and write it; return the written path.

    Destination is ``io_paths.lineage_path(run_id, job, builder.table)``. When
    ``s3_client`` is given the JSON is put to S3 via boto3; otherwise it is
    written under ``local_dir`` (preserving the per-table ``<table>.json`` name).

    The per-table path means the four files of an ``--all`` run never overwrite
    one another.
    """
    s3_uri = io_paths.lineage_path(builder.run_id, builder.job, builder.table, ctx)
    body = json.dumps(builder.to_dict(), indent=2, default=str)

    if s3_client is not None:
        # s3://bucket/key -> (bucket, key)
        bucket, key = _split_s3(s3_uri)
        s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
        return s3_uri

    # Local fallback: keep the per-table filename so two tables do not collide.
    import pathlib

    base = pathlib.Path(local_dir or ".")
    base.mkdir(parents=True, exist_ok=True)
    out = base / f"{builder.table}.json"
    out.write_text(body, encoding="utf-8")
    return str(out)


def register_glue_table(
    catalog: Any,
    database: str,
    table: str,
    location: str,
    fmt: str,
    columns: list[tuple[str, str]] | None = None,
) -> None:
    """Register (create-if-not-exists) a dataset in the Glue Data Catalog.

    Parameters
    ----------
    catalog:
        A boto3 Glue client.
    fmt:
        ``"parquet"`` or ``"iceberg"``.
        * ``parquet`` REQUIRES ``columns`` (ordered ``(name, glue_type)`` pairs)
          because Athena cannot read a schema-less Parquet table -- a missing
          ``columns`` raises ``ValueError``.
        * ``iceberg`` is a no-op create-if-not-exists: the Spark
          ``CREATE TABLE ... USING iceberg`` already registered the schema in
          Glue, so ``columns`` is ignored.

    boto3 is imported lazily (only needed when actually registering).
    """
    if fmt == "iceberg":
        # Iceberg tables self-register via Spark; nothing to do here.
        return

    if fmt != "parquet":
        raise ValueError(f"unsupported fmt {fmt!r}; expected 'parquet' or 'iceberg'")

    if not columns:
        raise ValueError(
            "register_glue_table requires `columns` for fmt='parquet' "
            "(StorageDescriptor.Columns must be populated for Athena)"
        )

    # Build the Glue StorageDescriptor.Columns from the ordered (name, type) pairs.
    glue_columns = [{"Name": name, "Type": glue_type} for name, glue_type in columns]
    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": glue_columns,
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                )
            },
        },
        "Parameters": {"classification": "parquet"},
    }

    # create-if-not-exists: try create, fall back to update on AlreadyExists.
    try:
        catalog.create_table(DatabaseName=database, TableInput=table_input)
    except Exception as exc:  # noqa: BLE001 - narrow by message (boto3 class varies)
        if "AlreadyExists" in type(exc).__name__ or "AlreadyExists" in str(exc):
            catalog.update_table(DatabaseName=database, TableInput=table_input)
        else:
            raise


def _split_s3(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key...`` into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri!r}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
