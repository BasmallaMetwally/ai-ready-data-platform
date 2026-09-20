"""Observability: audit trail, run metrics and failure alerting.

The first version of this project created ``raw.pipeline_audit`` in the DDL and
then never wrote a single row to it, and the daily DAG's docstring promised an
``on_failure_callback`` that did not exist. Both are implemented here.

Why an audit table at all, when Airflow already has a metadata database? Because
Airflow records *task* outcomes, not *data* outcomes. "The task succeeded" and
"the task loaded the expected number of rows" are different questions, and only
the second one catches a silently empty upstream.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

from pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)

Status = Literal["success", "failed", "skipped"]


@dataclass
class AuditRecord:
    run_id: str
    dag_id: str
    task_id: str
    logical_date: dt.date
    status: Status
    row_count: int | None = None
    duration_sec: float | None = None
    message: str | None = None

    def as_tuple(self) -> tuple:
        return (
            self.run_id,
            self.dag_id,
            self.task_id,
            self.logical_date,
            self.row_count,
            self.status,
            round(self.duration_sec, 2) if self.duration_sec is not None else None,
            (self.message or "")[:2000] or None,
        )


def write_audit(record: AuditRecord, settings: Settings | None = None) -> None:
    """Persist one audit row. Never raises.

    Observability failing must not fail the pipeline it observes — an audit
    insert that throws would turn a successful load into a failed task, which is
    strictly worse than losing one metric row.
    """
    from pipeline.load.warehouse import connection  # local import keeps DAG parsing fast

    try:
        with connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.pipeline_audit
                    (run_id, dag_id, task_id, logical_date, row_count,
                     status, duration_sec, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, task_id) DO UPDATE SET
                    row_count    = EXCLUDED.row_count,
                    status       = EXCLUDED.status,
                    duration_sec = EXCLUDED.duration_sec,
                    message      = EXCLUDED.message,
                    recorded_at  = now()
                """,
                record.as_tuple(),
            )
    except Exception as exc:  # noqa: BLE001 - observability must never break the run
        log.warning("failed to write audit record for %s: %s", record.task_id, exc)


@contextmanager
def audited(task_id: str, context: dict, settings: Settings | None = None) -> Iterator[dict]:
    """Wrap a unit of work so its outcome, duration and row count are recorded.

    Yields a mutable dict; set ``result["row_count"]`` inside the block and it
    ends up in the audit table.

        with audited("extract_btc", context) as result:
            rows = fetch()
            result["row_count"] = len(rows)
    """
    settings = settings or get_settings()
    started = dt.datetime.now(tz=dt.UTC)
    result: dict[str, Any] = {"row_count": None, "message": None}

    dag_run = context.get("dag_run")
    dag = context.get("dag")
    logical_date_value = context.get("logical_date")

    # Built as explicit typed locals rather than a dict unpacked with **base:
    # a bare `dict` has Any-typed values, so mypy could not verify that each
    # unpacked key actually matched AuditRecord's (str, str, str, date)
    # parameters and flagged every one of them as a potential mismatch.
    run_id: str = getattr(dag_run, "run_id", "manual")
    dag_id: str = dag.dag_id if dag is not None else "unknown"
    logical_date: dt.date = logical_date_value.date() if logical_date_value else dt.date.today()

    try:
        yield result
    except Exception as exc:
        write_audit(
            AuditRecord(
                run_id=run_id,
                dag_id=dag_id,
                task_id=task_id,
                logical_date=logical_date,
                status="failed",
                row_count=result.get("row_count"),
                duration_sec=(dt.datetime.now(tz=dt.UTC) - started).total_seconds(),
                message=f"{type(exc).__name__}: {exc}",
            ),
            settings,
        )
        raise
    else:
        write_audit(
            AuditRecord(
                run_id=run_id,
                dag_id=dag_id,
                task_id=task_id,
                logical_date=logical_date,
                status="success",
                row_count=result.get("row_count"),
                duration_sec=(dt.datetime.now(tz=dt.UTC) - started).total_seconds(),
                message=result.get("message"),
            ),
            settings,
        )


# --------------------------------------------------------------------- alerts
@dataclass
class Alert:
    """A structured failure notification, decoupled from any specific channel."""

    severity: Literal["warning", "critical"]
    title: str
    dag_id: str
    task_id: str
    logical_date: str
    detail: str
    log_url: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_slack_blocks(self) -> dict:
        emoji = "🔴" if self.severity == "critical" else "🟡"
        text = (
            f"{emoji} *{self.title}*\n"
            f"• DAG: `{self.dag_id}`\n"
            f"• Task: `{self.task_id}`\n"
            f"• Logical date: `{self.logical_date}`\n"
            f"• Detail: {self.detail[:500]}"
        )
        if self.log_url:
            text += f"\n• <{self.log_url}|View logs>"
        return {"text": text}


def send_alert(alert: Alert) -> None:
    """Deliver an alert to the configured webhook, or log it if none is set.

    Local development has no Slack workspace, so the default path is a structured
    log line. That keeps the callback safe to run everywhere rather than being
    something engineers disable on their laptop and forget to re-enable.
    """
    webhook = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook:
        log.error("ALERT [%s] %s", alert.severity.upper(), json.dumps(alert.to_slack_blocks()))
        return

    try:
        import requests

        response = requests.post(webhook, json=alert.to_slack_blocks(), timeout=10)
        if not response.ok:
            log.warning("alert webhook returned %s: %s", response.status_code, response.text[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("could not deliver alert: %s", exc)


def on_failure_callback(context: dict) -> None:
    """Airflow ``on_failure_callback``. Wire into default_args.

    Only pages on the *final* attempt. Alerting on every retry is how teams learn
    to ignore their alerts.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag = context.get("dag")

    if task_instance is not None:
        try:
            if task_instance.try_number <= task_instance.max_tries:
                log.info("retry remaining; suppressing alert for %s", task_instance.task_id)
                return
        except Exception:  # noqa: BLE001 - never let alerting logic crash a callback
            pass

    detail = f"{type(exception).__name__}: {exception}" if exception else "unknown failure"

    # The real bug this replaces: `severity = "critical" if ... else "warning"`
    # infers as plain `str`, not `Literal["warning", "critical"]`, so it typed
    # but was never actually guaranteed to be one of the two values Alert
    # accepts — a typo or a third branch added later would have passed mypy
    # and only broken at runtime inside `Alert.__init__`. Spelling it as an
    # if/else assigning the Literal type directly closes that gap.
    severity: Literal["warning", "critical"]
    severity = "critical" if "DataQuality" in detail or "Circuit" in detail else "warning"

    send_alert(
        Alert(
            severity=severity,
            title="Pipeline task failed after all retries",
            dag_id=dag.dag_id if dag is not None else "unknown",
            task_id=getattr(task_instance, "task_id", "unknown"),
            logical_date=str(context.get("logical_date", "unknown")),
            detail=detail,
            log_url=getattr(task_instance, "log_url", None),
        )
    )


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Airflow SLA-miss callback. A late pipeline is a degraded pipeline."""
    send_alert(
        Alert(
            severity="warning",
            title="SLA missed",
            dag_id=getattr(dag, "dag_id", "unknown"),
            task_id=", ".join(str(s.task_id) for s in slas) or "unknown",
            logical_date=str(getattr(slas[0], "execution_date", "unknown")) if slas else "unknown",
            detail=f"{len(slas)} task(s) exceeded their SLA; blocking: {blocking_task_list}",
        )
    )
