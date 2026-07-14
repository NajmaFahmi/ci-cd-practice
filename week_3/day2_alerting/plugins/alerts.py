"""
Alerting utilities for Airflow DAGs.

Provides structured failure and SLA-miss notifications. The notification channel
is pluggable: when a Slack webhook is configured it posts there, otherwise it
emits a structured log entry that downstream log aggreagators can pick up.
"""

from __future__ import annotations 
import json 
import logging
import os
from typing import Any
from datetime import datetime, timezone


## Create Logger
logger = logging.getLogger(__name__)

## Input Slack URL
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


## Create Function Sending Alert to Slack
def _emit(payload: dict[str, Any]) -> None:
    """Send the alert to the configured channel
    
    Falls back to structured logging when no webhook is configured,
    so the alerting path is always exercised even in local development.
    """

    if SLACK_WEBHOOK_URL:
        try:
            import requests
            requests.post(
                SLACK_WEBHOOK_URL,
                json={"text": _format_slack_message(payload)},
                timeout=10,
            )
        except Exception:
            logger.exception("Failed to deliver alert to Slack")
    
    logger.error("ALERT %s", json.dumps(payload, default=str))


## Create Format Slack Message
def _format_slack_message(payload: dict[str, Any]) -> str:
    lines = [f"{payload['alert_type']}*"]
    lines += [f"• {k}: `{v}`" for k, v in payload.items() if k != "alert_type"]
    return "\n".join(lines)


## Create Alert If Task Failed (Hard Failure --> on_failure_callback)
def alert_on_failure(context: dict[str, Any]) -> None:
    """Airflow ``on_failure_callback``: fires after all retries are exhausted."""
    try:
        ti = context["task_instance"]
        payload = {
            "alert_type": "TASK FAILURE",
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context.get("run_id"),
            "attempt": f"{ti.try_number}/{ti.max_tries + 1}",
            "logical_date": context.get("logical_date"),
            "exception": str(context.get("exception")),
            "log_url": ti.log_url,
            "duration_seconds": round((datetime.now(tz=timezone.utc) - ti.start_date).total_seconds(), 1)  if ti.start_date else None,
        }
        _emit(payload)
    except Exception:
        # A failing alert handler must never mask the original task failure
        logger.exception("alert_on_failure itself raised")


## Maximum Time for a Running Task (Soft Failure --> sla)
def alert_on_sla_miss(
    dag,
    task_list: str,
    blocking_task_list: str,
    slas: list,
    blocking_tis: list,
) -> None:
    """Airflow ``sla_miss_callback``: fires when a task breaches its SLA."""
    try:
        payload = {
            "alert_type": "SLA MISS",
            "dag_id": dag.dag_id,
            "tasks_missing_sla": [s.task_id for s in slas],
            "blocking_tasks": blocking_task_list.strip(),
        }
        _emit(payload)
    except Exception:
        logger.exception("alert_on_sla_miss itself raised")