import sys
import os
from prefect import flow

# ─────────────────────────────────────────────────────────────────────────────
# Ensure Python can find the `flows` package by adding the project root first:
PROJECT_ROOT = "/Users/pavelkuznecov/PycharmProjects/BI_project.v2"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ─────────────────────────────────────────────────────────────────────────────

# Import the actual flow object
from flows.elt_flows import initial_load_flow

if __name__ == "__main__":
    # ─────────────────────────────────────────────────────────────────────────
    # 1) Compute the absolute path to elt_flows.py so Prefect knows where to
    #    upload your code. Without this, Prefect will complain:
    #      “Either an image or remote storage location must be provided…”
    #    (see: https://docs.prefect.io/api-ref/prefect/flows/flow/#prefect.flows.Flow.deploy)
    #
    #    __file__ here is ".../BI_project.v2/flows/deploy_initial.py"
    # ─────────────────────────────────────────────────────────────────────────
    flows_dir = os.path.dirname(__file__)                      # e.g. "/Users/.../BI_project.v2/flows"
    flow_path = os.path.join(flows_dir, "elt_flows.py")        # "/Users/.../BI_project.v2/flows/elt_flows.py"

    # ─────────────────────────────────────────────────────────────────────────
    # 2) Deploy the “initial‐load” flow, pointing at our local elt_flows.py file:
    #    By passing `path=flow_path`, Prefect knows where to pick up the flow code.
    # ─────────────────────────────────────────────────────────────────────────
    initial_load_flow.deploy(
        name="initial-load",
        work_pool_name="default",      # must match your existing pool
        work_queue_name="default",     # must match your existing queue
        path=flow_path                 # ← this is the critical addition
    )

    print("✅ initial_load_flow deployed as 'initial-load'")
