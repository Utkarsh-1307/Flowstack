"""
Centralized failure alert hook. Attach to any DAG via:
    default_args = {"on_failure_callback": on_failure_alert}
Reads SLACK_WEBHOOK_URL / DISCORD_WEBHOOK_URL from Airflow Variables or env.
"""
import os
import requests
from datetime import datetime


def on_failure_alert(context: dict) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url
    exception = context.get("exception", "Unknown error")

    message = (
        f":red_circle: *Pipeline Failure*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Run date:* `{execution_date}`\n"
        f"*Error:* `{exception}`\n"
        f"*Logs:* {log_url}"
    )

    slack_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if slack_url:
        try:
            requests.post(slack_url, json={"text": message}, timeout=5)
        except requests.RequestException:
            pass

    discord_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if discord_url:
        try:
            requests.post(discord_url, json={"content": message}, timeout=5)
        except requests.RequestException:
            pass
