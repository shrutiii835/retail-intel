"""Run the whole medallion across all batches as ONE Databricks multi-task job.

Why a multi-task job rather than nine separate runs: a job cluster takes about
six minutes to start, and nine sequential runs would spend an hour doing nothing
but provisioning VMs. A multi-task job declares a single `job_clusters` entry
that every task shares, so the cluster starts once and all tasks run on it.

Task dependencies encode the medallion ordering explicitly — bronze before
silver before gold, and batch N before batch N+1 — which is the same ordering
the ADF pipeline enforces with activity dependencies.

    python scripts/dbx_medallion.py --storage-account <sa> --tenant-id <tid>
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))

from scripts.dbx_run import (  # noqa: E402
    NOTEBOOKS,
    WHEEL,
    WORKSPACE_URL,
    api,
    cluster_spec,
)

BATCHES = ["batch_01_initial", "batch_02_incremental", "batch_03_incremental"]
CLUSTER_KEY = "retailintel_shared"


def build_tasks(storage_account: str, skip: set[str]) -> list[dict]:
    lake = f"abfss://lakehouse@{storage_account}.dfs.core.windows.net"
    landing = f"abfss://landing@{storage_account}.dfs.core.windows.net"
    tasks: list[dict] = []
    previous: str | None = None

    for bi, batch in enumerate(BATCHES, start=1):
        for layer in ("bronze", "silver", "gold"):
            key = f"b{bi}_{layer}"
            if key in skip:
                # Already run successfully; keep the chain linked so ordering
                # across batches is still enforced.
                previous = previous
                continue
            params = {
                "batch_id": batch,
                "run_id": f"azure_{batch}_{layer}",
                "full_reload": "false",
                "lakehouse_root": lake,
            }
            if layer == "bronze":
                params["landing_root"] = landing

            task = {
                "task_key": key,
                "job_cluster_key": CLUSTER_KEY,
                "libraries": [{"whl": WHEEL}],
                "notebook_task": {
                    "notebook_path": NOTEBOOKS[layer],
                    "base_parameters": params,
                },
                "timeout_seconds": 3600,
            }
            if previous:
                task["depends_on"] = [{"task_key": previous}]
            tasks.append(task)
            previous = key
    return tasks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--storage-account", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--node-type", default="Standard_D4ads_v5")
    ap.add_argument("--skip", default="",
                    help="comma-separated task keys already completed, e.g. b1_bronze")
    a = ap.parse_args()

    skip = {s.strip() for s in a.skip.split(",") if s.strip()}
    tasks = build_tasks(a.storage_account, skip)

    # A shared job cluster is only available on a *created* job — the
    # runs/submit one-shot API rejects it ("Shared job cluster feature is not
    # supported in runs/submit API"). Creating the job is better anyway: it
    # shows up in the workspace UI as a real multi-task pipeline.
    spec = {
        "name": "RetailIntel — medallion (all batches)",
        "timeout_seconds": 10800,
        "max_concurrent_runs": 1,
        "job_clusters": [{
            "job_cluster_key": CLUSTER_KEY,
            "new_cluster": cluster_spec(a.node_type, a.storage_account, a.tenant_id),
        }],
        "tasks": tasks,
    }

    print(f"creating job: {len(tasks)} tasks on one shared {a.node_type} cluster")
    for t in tasks:
        dep = t.get("depends_on", [{}])[0].get("task_key", "-")
        print(f"  {t['task_key']:<12} after {dep}")

    created = api("/api/2.2/jobs/create", spec)
    if "_http_error" in created:
        raise SystemExit(f"job create failed: {created['_http_error']} {created['_body']}")
    job_id = created["job_id"]
    print(f"job_id={job_id}")

    r = api("/api/2.2/jobs/run-now", {"job_id": job_id})
    if "_http_error" in r:
        raise SystemExit(f"run-now failed: {r['_http_error']} {r['_body']}")
    run_id = r["run_id"]
    print(f"\nrun_id={run_id}")
    print(f"{WORKSPACE_URL}/#job/runs/{run_id}")

    # Poll, reporting each task as it reaches a terminal state.
    seen: dict[str, str] = {}
    start = time.time()
    while time.time() - start < 10800:
        d = api(f"/api/2.2/jobs/runs/get?run_id={run_id}", method="GET")
        if "_http_error" in d:
            time.sleep(20)
            continue
        for t in d.get("tasks", []):
            st = (t.get("status") or {}).get("state")
            k = t["task_key"]
            if st and seen.get(k) != st:
                seen[k] = st
                print(f"  [{int(time.time()-start):>4}s] {k:<12} {st}")
        top = (d.get("status") or {}).get("state")
        if top == "TERMINATED":
            td = (d.get("status") or {}).get("termination_details") or {}
            print(f"\nJOB TERMINATED: {td.get('code')}")
            for t in d.get("tasks", []):
                out = (t.get("notebook_output") or {}).get("result")
                tstate = (t.get("status") or {}).get("state")
                ttd = ((t.get("status") or {}).get("termination_details") or {})
                print(f"\n--- {t['task_key']} [{tstate} {ttd.get('code','')}] ---")
                if out:
                    try:
                        print(json.dumps(json.loads(out), indent=2)[:1400])
                    except Exception:
                        print(out[:800])
                elif ttd.get("message"):
                    print(ttd["message"][:700])
            raise SystemExit(0 if td.get("code") == "SUCCESS" else 1)
        time.sleep(20)


if __name__ == "__main__":
    main()
