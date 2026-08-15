"""Submit a RetailIntel notebook to Databricks as a one-time job run.

Used to verify the Azure deployment and to execute the medallion layers. Handles
the two things that bite on a free-trial subscription:

  * VM capacity stockouts — CentralIndia frequently refuses a given 4-core SKU,
    so the node type is chosen by trying a candidate list until one actually
    starts, rather than hardcoding one and failing.
  * Short-lived AAD tokens — the Databricks token is minted from the Azure CLI
    and refreshed on every poll, so a long cluster start cannot expire it
    mid-run.

Storage access uses a service principal whose credentials live in the
`retailintel` Databricks secret scope. Nothing secret appears in this file, in
the cluster spec that is printed, or in the repository — the Spark conf only
carries `{{secrets/...}}` references, which Databricks resolves at runtime.

    python scripts/dbx_run.py <notebook> [--batch B] [--full-reload] [--wait N]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

WORKSPACE_URL = "https://adb-7405607439445982.2.azuredatabricks.net"
DATABRICKS_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
SECRET_SCOPE = "retailintel"
WHEEL = "/Workspace/Shared/RetailIntel/retailintel-0.1.0-py3-none-any.whl"
SPARK_VERSION = "15.4.x-scala2.12"

# Tried in order until one starts. Free-trial subscriptions hit capacity
# stockouts constantly, and the failure only surfaces after cluster creation.
NODE_TYPE_CANDIDATES = [
    "Standard_D4ads_v5",
    "Standard_D4s_v5",
    "Standard_D4ds_v5",
    "Standard_E4ds_v5",
    "Standard_D4as_v5",
    "Standard_E4as_v5",
]

NOTEBOOKS = {
    "bronze": "/Shared/RetailIntel/01_bronze_ingest",
    "silver": "/Shared/RetailIntel/02_silver_build",
    "gold": "/Shared/RetailIntel/03_gold_build",
}


def token() -> str:
    return subprocess.run(
        ["az", "account", "get-access-token", "--resource", DATABRICKS_RESOURCE,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def api(path: str, payload: dict | None = None, method: str = "POST") -> dict:
    url = f"{WORKSPACE_URL}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": f"Bearer {token()}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        return json.loads(urllib.request.urlopen(req).read() or "{}")
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "_body": e.read().decode()[:600]}


def cluster_spec(node_type: str, storage_account: str, tenant_id: str) -> dict:
    sa = f"{storage_account}.dfs.core.windows.net"
    return {
        "spark_version": SPARK_VERSION,
        "node_type_id": node_type,
        "num_workers": 0,
        "custom_tags": {"ResourceClass": "SingleNode", "project": "RetailIntel"},
        "spark_conf": {
            # Single node: driver and executor in one JVM. 130K rows does not
            # need a cluster, and the free-trial quota is 4 vCPUs total.
            "spark.master": "local[*, 4]",
            "spark.databricks.cluster.profile": "singleNode",
            "spark.sql.shuffle.partitions": "8",
            # ADLS access via service principal OAuth. Values are secret
            # references, resolved by Databricks at cluster start.
            f"fs.azure.account.auth.type.{sa}": "OAuth",
            f"fs.azure.account.oauth.provider.type.{sa}":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{sa}":
                f"{{{{secrets/{SECRET_SCOPE}/sp-client-id}}}}",
            f"fs.azure.account.oauth2.client.secret.{sa}":
                f"{{{{secrets/{SECRET_SCOPE}/sp-client-secret}}}}",
            f"fs.azure.account.oauth2.client.endpoint.{sa}":
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        },
    }


def submit(layer: str, node_type: str, params: dict, storage_account: str,
           tenant_id: str) -> int | None:
    spec = {
        "run_name": f"retailintel-{layer}-{params.get('batch_id', 'na')}",
        "timeout_seconds": 5400,
        "new_cluster": cluster_spec(node_type, storage_account, tenant_id),
        "libraries": [{"whl": WHEEL}],
        "notebook_task": {
            "notebook_path": NOTEBOOKS[layer],
            "base_parameters": params,
        },
    }
    r = api("/api/2.2/jobs/runs/submit", spec)
    if "_http_error" in r:
        print(f"    submit failed: {r['_http_error']} {r['_body'][:200]}")
        return None
    return r.get("run_id")


def poll(run_id: int, timeout_s: int = 2700) -> dict:
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        r = api(f"/api/2.2/jobs/runs/get?run_id={run_id}", method="GET")
        if "_http_error" in r:
            time.sleep(15)
            continue
        st = r.get("status", {})
        state = st.get("state")
        if state != last:
            print(f"    {int(time.time() - start):>4}s  {state}")
            last = state
        if state == "TERMINATED":
            return r
        time.sleep(15)
    return {"status": {"state": "POLL_TIMEOUT"}}


def run_layer(layer: str, params: dict, storage_account: str, tenant_id: str) -> dict:
    """Submit, and on a capacity stockout retry with the next node type."""
    for node_type in NODE_TYPE_CANDIDATES:
        print(f"  [{layer}] submitting on {node_type}")
        run_id = submit(layer, node_type, params, storage_account, tenant_id)
        if run_id is None:
            continue
        result = poll(run_id)
        td = (result.get("status", {}).get("termination_details") or {})
        code = td.get("code", "")
        msg = td.get("message", "") or ""
        if code == "SUCCESS":
            print(f"  [{layer}] SUCCESS (run {run_id}, node {node_type})")
            return {"ok": True, "run_id": run_id, "node_type": node_type, "result": result}
        if "STOCKOUT" in msg.upper() or "not available" in msg or code == "CLUSTER_ERROR":
            print(f"  [{layer}] {node_type} unavailable — trying next node type")
            continue
        print(f"  [{layer}] FAILED code={code}")
        print(f"    {msg[:900]}")
        return {"ok": False, "run_id": run_id, "code": code, "message": msg}
    return {"ok": False, "code": "NO_CAPACITY",
            "message": "every candidate node type was unavailable"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("layer", choices=list(NOTEBOOKS) + ["all"])
    ap.add_argument("--batch", default="batch_01_initial")
    ap.add_argument("--full-reload", action="store_true")
    ap.add_argument("--storage-account", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--landing-root", default="")
    args = ap.parse_args()

    lake = f"abfss://lakehouse@{args.storage_account}.dfs.core.windows.net"
    params = {
        "batch_id": args.batch,
        "run_id": f"azure_{args.batch}",
        "full_reload": "true" if args.full_reload else "false",
        "lakehouse_root": lake,
    }
    if args.landing_root:
        params["landing_root"] = args.landing_root

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]
    results = {}
    for layer in layers:
        p = dict(params)
        if layer != "bronze":
            p.pop("landing_root", None)
        r = run_layer(layer, p, args.storage_account, args.tenant_id)
        results[layer] = r
        if not r["ok"]:
            print(f"\nSTOPPING — {layer} did not succeed")
            sys.exit(1)
        out = r["result"].get("notebook_output", {}).get("result")
        if out:
            print(f"    output: {out[:600]}")

    print("\nALL LAYERS SUCCEEDED")


if __name__ == "__main__":
    main()
