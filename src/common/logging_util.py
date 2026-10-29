"""logging_util.py -- structured, single-line JSON logging for observability.

EMR Serverless ships a job's stdout to CloudWatch / S3, so every log record is
emitted as ONE line of JSON. Downstream (CloudWatch Logs Insights, Athena over
the log bucket, or a human) can then filter on stable fields like ``event`` or
``run_id`` without parsing free-form text.

Stdlib only -- no Spark, no boto3 -- so this is fully usable and testable on the
bare host.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager

# -- Stable event enum (design S5.1) ---------------------------------------
# These are the canonical ``event`` tokens. Keeping them as named constants
# means a typo in a job is a NameError at author time, not a silently-wrong log.
EVENT_JOB_START = "job_start"
EVENT_READ_DONE = "read_done"
EVENT_DQ_START = "dq_start"
EVENT_DQ_PASS = "dq_pass"
EVENT_DQ_FAIL = "dq_fail"
EVENT_WRITE_DONE = "write_done"
EVENT_JOB_END = "job_end"

# Convenience tuple of the stable events (handy for tests / validation).
EVENTS = (
    EVENT_JOB_START,
    EVENT_READ_DONE,
    EVENT_DQ_START,
    EVENT_DQ_PASS,
    EVENT_DQ_FAIL,
    EVENT_WRITE_DONE,
    EVENT_JOB_END,
)

# Attribute names we copy off a LogRecord into the JSON payload. Anything passed
# via ``log_event(**fields)`` is attached to the record and merged in too.
_CONTEXT_FIELDS = ("run_id", "layer", "table")


class _JsonFormatter(logging.Formatter):
    """Format each record as a single compact JSON line.

    Always emits ``run_id, layer, table, event, message, ts``; any extra fields
    attached to the record (via ``log_event``) are merged in as well.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            # UTC ISO-8601 timestamp with a trailing Z.
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "run_id": getattr(record, "run_id", None),
            "layer": getattr(record, "layer", None),
            "table": getattr(record, "table", None),
            "event": getattr(record, "event", None),
            "message": record.getMessage(),
        }
        # Merge any ad-hoc fields passed through log_event (stored under .extra).
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, default=str, separators=(",", ":"))


def get_logger(run_id: str, layer: str, table: str | None = None) -> logging.Logger:
    """Return a stdlib logger that emits single-line JSON to stdout.

    The returned logger is bound to ``run_id``/``layer``/``table`` via a filter so
    every record it emits carries that context automatically. The logger name is
    unique per (run_id, layer, table) so repeated calls do not stack handlers.
    """
    name = f"ocbc.{run_id}.{layer}.{table or '-'}"
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Don't bubble up to the root logger (avoids duplicate, non-JSON lines).
    logger.propagate = False

    # Attach exactly one JSON handler + one context filter per logger.
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)

        def _inject_context(record: logging.LogRecord) -> bool:
            # Default the context fields when a caller logs without log_event.
            if not hasattr(record, "run_id"):
                record.run_id = run_id
            if not hasattr(record, "layer"):
                record.layer = layer
            if not hasattr(record, "table"):
                record.table = table
            if not hasattr(record, "event"):
                record.event = None
            return True

        logger.addFilter(_inject_context)

    return logger


def log_event(logger: logging.Logger, event: str, message: str = "", **fields: object) -> None:
    """Emit one structured record with a stable ``event`` token and extra fields.

    Parameters
    ----------
    logger:
        A logger from :func:`get_logger`.
    event:
        One of the stable ``EVENT_*`` tokens (free strings are allowed too, but
        prefer the constants).
    message:
        Optional human-readable message.
    **fields:
        Arbitrary extra key/values merged into the JSON payload (e.g.
        ``row_count=100``).
    """
    logger.info(message, extra={"event": event, "extra_fields": dict(fields)})


@contextmanager
def timed(logger: logging.Logger, event: str, **fields: object):
    """Context manager that logs ``<event>`` start/end and a ``duration_ms`` field.

    Usage::

        with timed(logger, "silver_write", table="customers"):
            df.writeTo(...).createOrReplace()

    Emits a ``<event>_start`` record on entry and a ``<event>_end`` record on
    exit carrying ``duration_ms``.
    """
    start = time.monotonic()
    log_event(logger, f"{event}_start", **fields)
    try:
        yield
    finally:
        duration_ms = int((time.monotonic() - start) * 1000)
        log_event(logger, f"{event}_end", duration_ms=duration_ms, **fields)
