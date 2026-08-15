"""Render and deploy the Data Factory definitions.

The JSON under `adf/` is deliberately parameterised — it carries no storage
account name, no workspace URL, no resource ids. Those are environment values,
not source, so they are resolved here from the Azure CLI at deploy time and
injected. That is what makes the same definitions deployable to a second
environment without editing a file.

Nothing secret is written. Both linked services authenticate with managed
identities, and the Databricks cluster reads ADLS through `{{secrets/...}}`
references that Databricks resolves at runtime.

    python scripts/deploy_adf.py --resource-group rg-retailintel \
        --factory adf-retailintel --workspace dbw-retailintel
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ADF = REPO / "adf"
WHEEL = "/Workspace/Shared/RetailIntel/retailintel-0.1.0-py3-none-any.whl"
SPARK_VERSION = "15.4.x-scala2.12"
NODE_TYPE = "Standard_D4ads_v5"
SECRET_SCOPE = "retailintel"

NOTEBOOK_PATHS = {
    "BronzeIngest": "/Shared/RetailIntel/01_bronze_ingest",
    "SilverBuild": "/Shared/RetailIntel/02_silver_build",
    "GoldBuild": "/Shared/RetailIntel/03_gold_build",
}


def az(*args: str) -> str:
    r = subprocess.run(["az", *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"az {' '.join(args)} failed:\n{r.stderr[:800]}")
    return r.stdout.strip()


def az_json(*args: str) -> dict:
    return json.loads(az(*args) or "{}")


def deploy(resource_group: str, factory: str, workspace: str) -> None:
    print("resolving environment values from Azure...")
    sa = az("storage", "account", "list", "--resource-group", resource_group,
            "--query", "[0].name", "-o", "tsv")
    tenant = az("account", "show", "--query", "tenantId", "-o", "tsv")
    ws = az_json("databricks", "workspace", "show", "--resource-group", resource_group,
                 "--name", workspace, "-o", "json")
    ws_url, ws_id = ws["workspaceUrl"], ws["id"]
    lakehouse = f"abfss://lakehouse@{sa}.dfs.core.windows.net"
    print(f"  storage account : {sa}")
    print(f"  databricks      : {ws_url}")
    print(f"  lakehouse root  : {lakehouse}")

    sa_host = f"{sa}.dfs.core.windows.net"

    # ---- linked services -------------------------------------------------
    ls_adls = {
        "type": "AzureBlobFS",
        "description": ("ADLS Gen2 lakehouse. Authenticates with the Data Factory "
                        "system-assigned managed identity - no account key."),
        "typeProperties": {"url": f"https://{sa_host}"},
    }

    ls_dbx = {
        "type": "AzureDatabricks",
        "description": ("Databricks compute. MSI auth (no PAT). Job cluster: created "
                        "for the run and terminated when it ends, so nothing is left "
                        "billing. Single node - the free-trial quota is 4 vCPUs."),
        "typeProperties": {
            "domain": f"https://{ws_url}",
            "authentication": "MSI",
            "workspaceResourceId": ws_id,
            "newClusterNodeType": NODE_TYPE,
            "newClusterNumOfWorker": "0",
            "newClusterVersion": SPARK_VERSION,
            "newClusterCustomTags": {"ResourceClass": "SingleNode", "project": "RetailIntel"},
            "newClusterSparkConf": {
                "spark.master": "local[*, 4]",
                "spark.databricks.cluster.profile": "singleNode",
                "spark.sql.shuffle.partitions": "8",
                # ADLS OAuth via service principal. These are secret
                # *references*; Databricks resolves them at cluster start.
                f"fs.azure.account.auth.type.{sa_host}": "OAuth",
                f"fs.azure.account.oauth.provider.type.{sa_host}":
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"fs.azure.account.oauth2.client.id.{sa_host}":
                    f"{{{{secrets/{SECRET_SCOPE}/sp-client-id}}}}",
                f"fs.azure.account.oauth2.client.secret.{sa_host}":
                    f"{{{{secrets/{SECRET_SCOPE}/sp-client-secret}}}}",
                f"fs.azure.account.oauth2.client.endpoint.{sa_host}":
                    f"https://login.microsoftonline.com/{tenant}/oauth2/token",
            },
        },
    }

    # ---- datasets ---------------------------------------------------------
    def dataset(container: str, prefix: str) -> dict:
        folder = (f"@concat('{prefix}', dataset().batch_id, '/', dataset().source_name)"
                  if prefix else
                  "@concat(dataset().batch_id, '/', dataset().source_name)")
        return {
            "linkedServiceName": {"referenceName": "ls_adls_gen2",
                                  "type": "LinkedServiceReference"},
            "parameters": {"batch_id": {"type": "string"},
                           "source_name": {"type": "string"}},
            "type": "DelimitedText",
            "typeProperties": {
                "location": {"type": "AzureBlobFSLocation", "fileSystem": container,
                             "folderPath": {"value": folder, "type": "Expression"}},
                "columnDelimiter": ",", "escapeChar": "\\",
                "firstRowAsHeader": True, "quoteChar": "\"",
            },
            "schema": [],
        }

    # ---- pipeline ---------------------------------------------------------
    pipeline = json.loads((ADF / "pipeline" / "pl_retailintel_medallion.json").read_text())
    props = pipeline["properties"]
    for act in props["activities"]:
        if act["type"] == "DatabricksNotebook":
            act["typeProperties"]["notebookPath"] = NOTEBOOK_PATHS[act["name"]]
            act["typeProperties"]["baseParameters"]["lakehouse_root"] = lakehouse
            # Ship the project code as a versioned wheel rather than loose files.
            act["typeProperties"]["libraries"] = [{"whl": WHEEL}]

    # ---- deploy -----------------------------------------------------------
    def put(kind: str, name: str, body: dict, flag: str) -> None:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(body, f)
            path = f.name
        az("datafactory", kind, "create", "--resource-group", resource_group,
           "--factory-name", factory, flag, name,
           ("--properties" if kind != "pipeline" else "--pipeline"), f"@{path}")
        print(f"  deployed {kind}: {name}")

    print("\ndeploying...")
    put("linked-service", "ls_adls_gen2", ls_adls, "--linked-service-name")
    put("linked-service", "ls_databricks", ls_dbx, "--linked-service-name")
    put("dataset", "ds_landing_csv", dataset("landing", ""), "--dataset-name")
    put("dataset", "ds_raw_csv", dataset("lakehouse", "raw/"), "--dataset-name")
    put("pipeline", "pl_retailintel_medallion", props, "--name")

    print("\ndeployed. run it with:")
    print(f"  az datafactory pipeline create-run --resource-group {resource_group} \\")
    print(f"    --factory-name {factory} --name pl_retailintel_medallion \\")
    print("    --parameters '{\"batch_id\":\"batch_01_initial\",\"full_reload\":true}'")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource-group", default="rg-retailintel")
    ap.add_argument("--factory", default="adf-retailintel")
    ap.add_argument("--workspace", default="dbw-retailintel")
    a = ap.parse_args()
    deploy(a.resource_group, a.factory, a.workspace)


if __name__ == "__main__":
    main()
