"""
DAG 9: archive_and_purge_users
Category: Data Lifecycle & Cleanup

WHAT IT DOES:
  On the 1st of each month at 04:00 AM, identifies users who haven't generated
  any events in the past 180 days (inactive users). Exports their records to
  Parquet in /data/gold/archived_users/ as a safe backup, then soft-deletes
  them from the users table by setting a deleted_at timestamp.

WHY WE USE IT:
  GDPR and data minimization principles require removing user data that is no
  longer needed. Archiving before deleting ensures we can restore data if needed
  (e.g., a user reactivates or there was a bug). The Parquet archive is
  immutable, timestamped evidence of what we deleted and when.

KEY AIRFLOW CONCEPT TAUGHT:
  BranchPythonOperator for "guard before destructive action" pattern —
  the branch checks if there's anything to archive. If no inactive users exist,
  it routes to a safe no-op path instead of running the delete SQL.
  This is a critical pattern: always verify before deleting, and make
  the delete conditional on real data existing.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

import asyncpg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from alerts import on_failure_alert

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}

INACTIVITY_DAYS = 180   # users with no events in 180 days are candidates for archiving


def _identify_inactive_users(**context):
    """
    Finds users who have NO events in the past INACTIVITY_DAYS.
    Uses LEFT JOIN + NULL check to find users with no matching events.
    This is safer than NOT IN with a subquery (which can behave unexpectedly
    when the subquery contains NULL values).
    """
    async def _query():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch(
                f"""
                SELECT u.id::text AS user_id, u.email, u.created_at
                FROM users u
                LEFT JOIN raw_events re
                    ON re.user_id = u.id
                   AND re.created_at >= NOW() - INTERVAL '{INACTIVITY_DAYS} days'
                WHERE re.id IS NULL
                  AND u.created_at < NOW() - INTERVAL '{INACTIVITY_DAYS} days'
                LIMIT 10000
                """
            )
        finally:
            await conn.close()
        return [dict(r) for r in rows]

    inactive_users = asyncio.run(_query())
    count = len(inactive_users)
    print(f"Found {count} inactive users (no events in {INACTIVITY_DAYS} days)")
    context["ti"].xcom_push(key="inactive_count", value=count)
    # Store user_ids as strings for the delete SQL
    context["ti"].xcom_push(key="user_ids", value=[u["user_id"] for u in inactive_users])
    return count


def _branch_has_inactive(**context):
    count = context["ti"].xcom_pull(task_ids="identify_inactive_users", key="inactive_count") or 0
    return "export_to_parquet" if count > 0 else "no_inactive_users_log"


def _export_to_parquet(**context):
    """
    Exports inactive user records to Parquet BEFORE deleting them.
    This is the safety net — the archive proves what we deleted and allows
    restoration if needed.

    The archive path includes the run date so each monthly archive is separate:
    /data/gold/archived_users/2026-06-01/users.parquet
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    user_ids = context["ti"].xcom_pull(task_ids="identify_inactive_users", key="user_ids") or []

    async def _fetch():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch(
                "SELECT id::text AS id, email, created_at FROM users WHERE id::text = ANY($1)",
                user_ids,
            )
        finally:
            await conn.close()
        return [dict(r) for r in rows]

    users = asyncio.run(_fetch())
    if not users:
        print("No user records fetched — nothing to archive")
        return

    # Write to Parquet archive
    archive_date = context["ds"]
    out_dir = f"/data/gold/archived_users/{archive_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/users.parquet"

    table = pa.table({
        "id": [u["id"] for u in users],
        "email": [u["email"] for u in users],
        "created_at": [u["created_at"] for u in users],
        "archived_at": [datetime.utcnow()] * len(users),
        "archive_reason": [f"inactive_{INACTIVITY_DAYS}d"] * len(users),
    })
    pq.write_table(table, out_path)
    print(f"Archived {len(users)} users to {out_path}")


def _soft_delete_users(**context):
    """
    Soft-deletes users by setting deleted_at timestamp.
    Adds the column if it doesn't exist (ALTER TABLE IF NOT EXISTS pattern).

    WHY SOFT DELETE?
    Hard DELETE is irreversible. Soft delete (setting a timestamp) lets us:
    - Filter out deleted users in queries (WHERE deleted_at IS NULL)
    - Restore accidentally deleted users (SET deleted_at = NULL)
    - Audit when each user was deleted
    """
    user_ids = context["ti"].xcom_pull(task_ids="identify_inactive_users", key="user_ids") or []

    async def _delete():
        db_url = os.environ["DATABASE_URL"].replace("+asyncpg", "")
        conn = await asyncpg.connect(db_url)
        try:
            # Add deleted_at column if it doesn't exist yet
            await conn.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE"
            )
            # Soft-delete the identified inactive users
            result = await conn.execute(
                "UPDATE users SET deleted_at = NOW() WHERE id::text = ANY($1) AND deleted_at IS NULL",
                user_ids,
            )
            print(f"Soft-deleted users: {result}")
        finally:
            await conn.close()

    asyncio.run(_delete())


def _log_purge_audit(**context):
    count = context["ti"].xcom_pull(task_ids="identify_inactive_users", key="inactive_count") or 0
    print(
        f"Monthly user purge complete: {count} users archived and soft-deleted "
        f"(inactivity > {INACTIVITY_DAYS} days, run date: {context['ds']})"
    )


with DAG(
    dag_id="archive_and_purge_users",
    default_args=default_args,
    schedule_interval="0 4 1 * *",   # 1st of each month, 04:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lifecycle", "gdpr", "archive", "cleanup"],
    doc_md=__doc__,
) as dag:

    identify_inactive_users = PythonOperator(
        task_id="identify_inactive_users",
        python_callable=_identify_inactive_users,
    )

    branch_has_inactive = BranchPythonOperator(
        task_id="branch_has_inactive",
        python_callable=_branch_has_inactive,
    )

    # ── Branch A: Archive then soft-delete ────────────────────────────────────
    export_to_parquet = PythonOperator(
        task_id="export_to_parquet",
        python_callable=_export_to_parquet,
    )

    soft_delete_users = PythonOperator(
        task_id="soft_delete_users",
        python_callable=_soft_delete_users,
    )

    log_purge_audit = PythonOperator(
        task_id="log_purge_audit",
        python_callable=_log_purge_audit,
    )

    # ── Branch B: Nothing to do ───────────────────────────────────────────────
    no_inactive_users_log = EmptyOperator(task_id="no_inactive_users_log")

    purge_complete = EmptyOperator(
        task_id="purge_complete",
        trigger_rule="none_failed_min_one_success",
    )

    identify_inactive_users >> branch_has_inactive
    branch_has_inactive >> export_to_parquet >> soft_delete_users >> log_purge_audit >> purge_complete
    branch_has_inactive >> no_inactive_users_log >> purge_complete
