"""Tests for auditing and alerting.

The governing rule under test: observability must never break the thing it
observes. Every failure path here has to degrade quietly.
"""

from __future__ import annotations

import datetime as dt

import pytest

from pipeline.observability import Alert, AuditRecord, audited, send_alert


def test_audit_record_serialises_in_column_order():
    record = AuditRecord(
        run_id="run-1",
        dag_id="d",
        task_id="t",
        logical_date=dt.date(2026, 9, 1),
        status="success",
        row_count=42,
        duration_sec=1.23456,
    )
    values = record.as_tuple()
    assert values[0] == "run-1"
    assert values[4] == 42
    assert values[5] == "success"
    assert values[6] == 1.23  # rounded for the numeric(10,2) column


def test_long_messages_are_truncated():
    record = AuditRecord(
        run_id="r",
        dag_id="d",
        task_id="t",
        logical_date=dt.date(2026, 9, 1),
        status="failed",
        message="x" * 5000,
    )
    assert len(record.as_tuple()[7]) == 2000


def test_empty_message_becomes_null():
    record = AuditRecord(
        run_id="r",
        dag_id="d",
        task_id="t",
        logical_date=dt.date(2026, 9, 1),
        status="success",
        message="",
    )
    assert record.as_tuple()[7] is None


def _context() -> dict:
    class _Dag:
        dag_id = "test_dag"

    class _Run:
        run_id = "test_run"

    return {
        "dag": _Dag(),
        "dag_run": _Run(),
        "logical_date": dt.datetime(2026, 9, 1, tzinfo=dt.UTC),
    }


def test_audited_yields_a_mutable_result(monkeypatch):
    written: list[AuditRecord] = []
    monkeypatch.setattr("pipeline.observability.write_audit", lambda r, s=None: written.append(r))

    with audited("my_task", _context()) as result:
        result["row_count"] = 7

    assert written[0].status == "success"
    assert written[0].row_count == 7
    assert written[0].duration_sec is not None


def test_audited_records_failure_and_reraises(monkeypatch):
    written: list[AuditRecord] = []
    monkeypatch.setattr("pipeline.observability.write_audit", lambda r, s=None: written.append(r))

    with pytest.raises(ValueError, match="boom"), audited("my_task", _context()):
        raise ValueError("boom")

    assert written[0].status == "failed"
    assert "ValueError: boom" in written[0].message


def test_audit_write_failure_does_not_break_the_task(monkeypatch, caplog):
    """The critical invariant: a broken audit table must not fail a good load."""

    def _explode(*_args, **_kwargs):
        raise ConnectionError("warehouse unreachable")

    monkeypatch.setattr("pipeline.load.warehouse.connection", _explode)
    from pipeline.observability import write_audit

    write_audit(
        AuditRecord(
            run_id="r",
            dag_id="d",
            task_id="t",
            logical_date=dt.date(2026, 9, 1),
            status="success",
        )
    )  # must not raise


def test_alert_renders_slack_payload():
    alert = Alert(
        severity="critical",
        title="Boom",
        dag_id="d",
        task_id="t",
        logical_date="2026-09-01",
        detail="something broke",
        log_url="http://logs/1",
    )
    text = alert.to_slack_blocks()["text"]
    assert "🔴" in text
    assert "Boom" in text
    assert "http://logs/1" in text


def test_warning_alerts_use_a_different_marker():
    alert = Alert(
        severity="warning",
        title="Late",
        dag_id="d",
        task_id="t",
        logical_date="2026-09-01",
        detail="slow",
    )
    assert "🟡" in alert.to_slack_blocks()["text"]


def test_send_alert_falls_back_to_logging_without_a_webhook(monkeypatch, caplog):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    alert = Alert(
        severity="critical",
        title="Boom",
        dag_id="d",
        task_id="t",
        logical_date="2026-09-01",
        detail="x",
    )
    with caplog.at_level("ERROR"):
        send_alert(alert)
    assert "ALERT [CRITICAL]" in caplog.text
