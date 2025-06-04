import sys
import os
from prefect.server.schemas.schedules import CronSchedule

# ─────────────────────────────────────────────────────────────────────────────
# Ensure Python can find the `flows` package by adding the project root first:
PROJECT_ROOT = "/Users/pavelkuznecov/PycharmProjects/BI_project.v2"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ─────────────────────────────────────────────────────────────────────────────

from flows.etl_flows import incremental_load_flow

if __name__ == "__main__":
    # ─────────────────────────────────────────────────────────────────────────
    # 1) Compute the absolute path to etl_flows.py so Prefect knows where to
    #    upload your code. (Same rationale as deploy_initial.py above.)
    # ─────────────────────────────────────────────────────────────────────────
    flows_dir = os.path.dirname(__file__)                                  # e.g. "/Users/.../BI_project.v2/flows"
    flow_path = os.path.join(flows_dir, "etl_flows.py")                    # "/Users/.../BI_project.v2/flows/etl_flows.py"

    # 2) Deploy the nightly incremental‐load flow on a daily Cron at 02:00 AM
    incremental_load_flow.deploy(
        name="daily-incremental-load",
        schedule=CronSchedule(
            cron="0 2 * * *",            # 02:00 AM every day
            timezone="Europe/Sarajevo"   # your local timezone
        ),
        work_pool_name="default",       # must match your existing pool
        work_queue_name="default"           # ← point at the same etl_flows.py file
    )

    print("✅ incremental_load_flow deployed as 'daily-incremental-load'")
