# File: flows/etl_flows.py

import sys
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# Ensure Prefect can import from your existing project codebase.
PROJECT_ROOT = "/Users/pavelkuznecov/PycharmProjects/BI_project.v2"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ─────────────────────────────────────────────────────────────────────────────

from prefect import flow, task
from warehouse.etl import run_incremental_load


# ─────────────────────────────────────────────────────────────────────────────
@task(name="incremental_load_task", retries=1, retry_delay_seconds=300)
def incremental_load(batch_id: int):
    run_incremental_load(batch_id)
    return f"Incremental load completed for batch {batch_id}"


# ─────────────────────────────────────────────────────────────────────────────
@flow(name="incremental_load_flow")
def incremental_load_flow(execution_date: date = date.today()):
    """
    This flow does exactly one incremental load, using the passed execution_date
    to compute a batch_id (YYYYMMDD). When served, Prefect will launch a new
    run every 30 seconds (see the `serve(...)` call below).
    """
    batch_id = int(execution_date.strftime("%Y%m%d"))
    message = incremental_load(batch_id)
    print(message)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # -------------------------------------------------------
    # 1) If you run `python flows/etl_flows.py` directly,
    #    PrefectOS will start a “local process” agent that:
    #      • registers this flow as a deployment,
    #      • polls the Prefect server (Orion) for scheduled work,
    #      • and automatically enqueues one new run every 30 seconds.
    #
    # 2) When you CTRL+C to stop this Python process, Prefect will pause
    #    the 30-second schedule automatically (so you don’t get “ghost” runs).
    #
    # 3) If you want the schedule to survive process restarts, pass
    #    `pause_on_shutdown=False` below.
    # -------------------------------------------------------
    incremental_load_flow.serve(
        name="incremental-load-every-30m",
        interval=1800,              # <— enqueue a new run every 30 minuted
        tags=["bi_project"],      # optional, for filtering/logging
        pause_on_shutdown=False,  # if True (the default), stopping this Python process
                                  # will pause (disable) the schedule; set False to keep it active
    )
