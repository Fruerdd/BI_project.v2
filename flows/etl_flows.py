# File: flows/etl_flows.py

import sys
import os
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# Ensure Prefect can import from your existing project codebase.
PROJECT_ROOT = "/Users/pavelkuznecov/PycharmProjects/BI_project.v2"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ─────────────────────────────────────────────────────────────────────────────

from prefect import flow, task
from warehouse.etl import run_incremental_load
from notifications.telegram import send_telegram_message

# ─────────────────────────────────────────────────────────────────────────────
@task(name="notify_start", retries=0, retry_delay_seconds=0, log_prints=True)
def notify_start(batch_id: int):
    """
    Send a “starting” message to Telegram before the incremental load.
    Uses the hardcoded bot token and chat ID in notifications/telegram.py.
    """
    message = f"🚀 Starting incremental load for batch {batch_id}"
    try:
        send_telegram_message(message)
    except Exception:
        pass  # Suppress Telegram errors to avoid failing the flow


# ─────────────────────────────────────────────────────────────────────────────
@task(name="incremental_load_task", retries=1, retry_delay_seconds=300, log_prints=True)
def incremental_load(batch_id: int):
    """
    Execute the existing incremental load logic.
    Prefect will retry this task once after 5 minutes if it raises.
    """
    run_incremental_load(batch_id)
    return f"Incremental load completed for batch {batch_id}"


# ─────────────────────────────────────────────────────────────────────────────
@task(name="notify_success", retries=0, retry_delay_seconds=0, log_prints=True)
def notify_success(batch_id: int):
    """
    Send a “success” message to Telegram. Suppress any errors.
    """
    message = f"✅ Incremental load succeeded for batch {batch_id}"
    try:
        send_telegram_message(message)
    except Exception:
        pass  # Suppress Telegram errors


# ─────────────────────────────────────────────────────────────────────────────
@task(name="notify_failure", retries=0, retry_delay_seconds=0, log_prints=True)
def notify_failure(batch_id: int, error_msg: str):
    """
    Send a “failure” message to Telegram, including the error details. Suppress errors.
    """
    message = f"❌ Incremental load FAILED for batch {batch_id}\nError: {error_msg}"
    try:
        send_telegram_message(message)
    except Exception:
        pass  # Suppress Telegram errors


# ─────────────────────────────────────────────────────────────────────────────
@flow(name="incremental_load_flow")
def incremental_load_flow(execution_date: date = date.today()):
    """
    1) Compute batch_id from execution_date (YYYYMMDD).
    2) Send “starting” message to Telegram.
    3) Execute incremental_load_task (with retry).
    4) On success: send “success” message.
    5) On exception: send “failure” message and re‐raise.
    """
    batch_id = int(execution_date.strftime("%Y%m%d"))

    # 1) Notify "starting"
    notify_start(batch_id)

    # 2) Run incremental load inside try/except
    try:
        msg = incremental_load(batch_id)
        notify_success(batch_id)
        print(msg)
    except Exception as e:
        err = str(e)
        notify_failure(batch_id, err)
        raise


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # -------------------------------------------------------
    # Launch Prefect’s local agent:
    #   • Enqueue a new run every 5 minutes (300 seconds)
    #   • Runs in this process until CTRL+C
    # -------------------------------------------------------
    incremental_load_flow.serve(
        name="incremental-load-every-5m",
        interval=300,            # 300 seconds = 5 minutes
        tags=["bi_project"],
        pause_on_shutdown=False,
    )
