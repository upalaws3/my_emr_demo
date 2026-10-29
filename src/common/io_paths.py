"""io_paths.py -- the SINGLE place that builds every S3 path and table name.

All functions here are PURE STRING builders: no AWS calls, no Spark, no file
I/O. That keeps them trivially unit-testable on the bare host and guarantees
there is exactly one definition of each path layout (design S2.2 / S5.8).

Bucket + database names are deterministic from ``project``/``env`` (design S2),
so these helpers accept a small ``IoContext`` carrying the resolved base names.
A caller builds the context once (usually from ``config_loader.Config`` plus the
AWS account id) and passes it to each helper. A convenience default context with
placeholder values keeps the layout testable without any AWS lookup.

Per-table keying (design Finding 1): ``lineage_path``, ``quality_path`` and
``audit_json_path`` ALL take a ``table`` argument so a 4-table run writes four
distinct files that never collide on a single ``<job>.json``.
"""

from __future__ import annotations

from dataclasses import dataclass

# Logical medallion layers that map to an S3 prefix under the data-lake bucket.
_LAYER_PREFIXES = ("landing", "bronze", "silver", "canonical", "gold")


@dataclass(frozen=True)
class IoContext:
    """Resolved base names needed to build concrete paths/table identifiers.

    Parameters
    ----------
    datalake_bucket:
        Fully-resolved data-lake bucket name (base name + account id), e.g.
        ``ocbc-poc-dev-datalake-123456789012``.
    db_prefix:
        Glue database prefix, e.g. ``ocbc_poc_dev`` (layer appended with ``_``).
    catalog:
        Iceberg Spark catalog name (``glue_catalog`` in the baseline).
    """

    datalake_bucket: str = "ocbc-poc-dev-datalake-000000000000"
    db_prefix: str = "ocbc_poc_dev"
    catalog: str = "glue_catalog"


# A ready-made context with placeholder values. Real callers build their own
# from config + account id; tests and docs can rely on this stable default.
DEFAULT_CONTEXT = IoContext()


def _base(ctx: IoContext | None) -> IoContext:
    """Return ``ctx`` or the module default when ``None`` is passed."""
    return ctx if ctx is not None else DEFAULT_CONTEXT


def s3_layer_prefix(layer: str, table: str, ctx: IoContext | None = None) -> str:
    """Return ``s3://<bucket>/<layer>/<table>/`` for a medallion layer.

    Raises ``ValueError`` for an unknown layer so a typo fails fast rather than
    silently writing to a stray prefix.
    """
    if layer not in _LAYER_PREFIXES:
        raise ValueError(f"unknown layer {layer!r}; expected one of {_LAYER_PREFIXES}")
    c = _base(ctx)
    return f"s3://{c.datalake_bucket}/{layer}/{table}/"


def warehouse_uri(ctx: IoContext | None = None) -> str:
    """Return the Iceberg warehouse root ``s3://<bucket>/warehouse/``.

    Iceberg/Glue manages physical table data under this prefix; the human-facing
    layer prefixes above are the logical contract.
    """
    c = _base(ctx)
    return f"s3://{c.datalake_bucket}/warehouse/"


def lineage_path(run_id: str, job: str, table: str, ctx: IoContext | None = None) -> str:
    """Return ``s3://<bucket>/lineage/<run_id>/<job>/<table>.json`` (one per table)."""
    c = _base(ctx)
    return f"s3://{c.datalake_bucket}/lineage/{run_id}/{job}/{table}.json"


def quality_path(run_id: str, job: str, table: str, ctx: IoContext | None = None) -> str:
    """Return ``s3://<bucket>/quality/<run_id>/<job>/<table>.json`` (one per table)."""
    c = _base(ctx)
    return f"s3://{c.datalake_bucket}/quality/{run_id}/{job}/{table}.json"


def audit_json_path(run_id: str, job: str, table: str, ctx: IoContext | None = None) -> str:
    """Return ``s3://<bucket>/audit/<run_id>/<job>/<table>.json`` (durable JSON mirror)."""
    c = _base(ctx)
    return f"s3://{c.datalake_bucket}/audit/{run_id}/{job}/{table}.json"


def glue_db(layer: str, ctx: IoContext | None = None) -> str:
    """Return the Glue database name for a layer, e.g. ``ocbc_poc_dev_silver``.

    Uses underscores because Athena/Hive identifiers disallow hyphens (S2.3).
    """
    if layer not in _LAYER_PREFIXES + ("audit",):
        raise ValueError(f"unknown layer {layer!r} for glue_db")
    c = _base(ctx)
    return f"{c.db_prefix}_{layer}"


def iceberg_table(layer: str, table: str, ctx: IoContext | None = None) -> str:
    """Return the fully-qualified Iceberg identifier ``glue_catalog.<db>.<table>``.

    This is what ``spark.table(...)`` / ``CREATE TABLE ... USING iceberg`` use.
    """
    c = _base(ctx)
    return f"{c.catalog}.{glue_db(layer, ctx)}.{table}"
