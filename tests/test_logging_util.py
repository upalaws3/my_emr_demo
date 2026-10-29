"""Unit tests for src/common/logging_util.py (stdlib only, no Spark)."""

import json
import logging

from src.common import logging_util


def _capture(logger):
    """Attach an in-memory handler sharing the logger's JSON formatter."""
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(self.format(record))

    handler = _Capture()
    # Reuse the JSON formatter already on the logger's stdout handler.
    handler.setFormatter(logger.handlers[0].formatter)
    logger.addHandler(handler)
    return records


def test_logger_emits_single_line_json_with_required_keys():
    logger = logging_util.get_logger("RUN1", "silver", "customers")
    records = _capture(logger)

    logging_util.log_event(logger, logging_util.EVENT_READ_DONE, "read ok", row_count=100)

    assert len(records) == 1
    line = records[0]
    assert "\n" not in line  # single line
    payload = json.loads(line)
    for key in ("run_id", "layer", "table", "event", "message", "ts"):
        assert key in payload
    assert payload["run_id"] == "RUN1"
    assert payload["layer"] == "silver"
    assert payload["table"] == "customers"
    assert payload["event"] == "read_done"
    assert payload["row_count"] == 100  # extra field merged in


def test_timed_emits_start_and_end_with_duration():
    logger = logging_util.get_logger("RUN2", "bronze")
    records = _capture(logger)

    with logging_util.timed(logger, "write", table="accounts"):
        pass

    payloads = [json.loads(r) for r in records]
    events = [p["event"] for p in payloads]
    assert "write_start" in events
    assert "write_end" in events
    end = next(p for p in payloads if p["event"] == "write_end")
    assert "duration_ms" in end
    assert isinstance(end["duration_ms"], int)
