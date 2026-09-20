"""Tests for the shared logging configuration.

This module exists so every task's logs are queryable JSON in a real log
collector; the tests check that promise directly rather than just checking
that ``configure()`` runs without raising.
"""

from __future__ import annotations

import json
import logging

from pipeline.logging_conf import JsonFormatter, configure


def _make_record(**overrides) -> logging.LogRecord:
    defaults = {
        "name": "pipeline.test",
        "level": logging.INFO,
        "pathname": __file__,
        "lineno": 1,
        "msg": "hello %s",
        "args": ("world",),
        "exc_info": None,
    }
    defaults.update(overrides)
    return logging.LogRecord(**defaults)


def test_format_produces_valid_json_with_the_core_fields():
    line = JsonFormatter().format(_make_record())
    payload = json.loads(line)  # raises if it is not valid JSON
    assert payload["level"] == "INFO"
    assert payload["logger"] == "pipeline.test"
    assert payload["message"] == "hello world"
    assert "ts" in payload


def test_message_interpolation_happens_before_serialisation():
    """getMessage() must run %-formatting; a raw template would leak "%s" into logs."""
    line = JsonFormatter().format(_make_record(msg="loaded %s rows for %s", args=(42, "BTCUSDT")))
    payload = json.loads(line)
    assert payload["message"] == "loaded 42 rows for BTCUSDT"


def test_exception_info_is_included_when_present():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = _make_record(exc_info=sys.exc_info())
    payload = json.loads(JsonFormatter().format(record))
    assert "exception" in payload
    assert "ValueError: boom" in payload["exception"]


def test_exception_key_is_absent_when_there_is_no_exception():
    payload = json.loads(JsonFormatter().format(_make_record()))
    assert "exception" not in payload


def test_known_extra_fields_are_promoted_to_top_level_keys():
    """Fields set via `logger.info(..., extra={...})` should be queryable
    columns in the log collector, not buried inside the message string."""
    record = _make_record()
    record.symbol = "BTCUSDT"
    record.row_count = 288
    payload = json.loads(JsonFormatter().format(record))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["row_count"] == 288


def test_unlisted_extra_fields_are_not_leaked():
    """Only the allow-listed keys are promoted; an arbitrary attribute someone
    attaches to a record (or that logging itself sets) must not appear."""
    record = _make_record()
    record.some_other_field = "should not appear"
    payload = json.loads(JsonFormatter().format(record))
    assert "some_other_field" not in payload


def test_non_ascii_messages_are_preserved_not_escaped():
    """ensure_ascii=False matters: this pipeline's own docs are in Arabic in
    places, and \\uXXXX-escaped logs are painful to grep."""
    line = JsonFormatter().format(_make_record(msg="تم تحميل %s صف", args=(10,)))
    assert "تم تحميل" in line  # would be \u062a\u0645... if escaped


def test_configure_installs_exactly_one_handler_on_the_root_logger():
    configure()
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)


def test_configure_is_idempotent():
    """Calling configure() twice (e.g. once per Airflow task retry within the
    same worker process) must not accumulate duplicate handlers, which would
    otherwise print every log line multiple times."""
    configure()
    configure()
    configure()
    assert len(logging.getLogger().handlers) == 1


def test_configure_can_produce_plain_text_instead_of_json():
    configure(as_json=False)
    root = logging.getLogger()
    assert not isinstance(root.handlers[0].formatter, JsonFormatter)


def test_configure_quiets_noisy_third_party_loggers():
    configure()
    assert logging.getLogger("urllib3").level == logging.WARNING
    assert logging.getLogger("botocore").level == logging.WARNING


def test_configure_respects_the_requested_level():
    configure(level=logging.DEBUG)
    assert logging.getLogger().level == logging.DEBUG
