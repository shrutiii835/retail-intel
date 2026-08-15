# Databricks notebook source
# MAGIC %md
# MAGIC # RetailIntel — Bronze ingestion
# MAGIC
# MAGIC Invoked by the ADF activity `BronzeIngest` in `pl_retailintel_medallion`.
# MAGIC
# MAGIC Reads the landed CSV batch, applies the **watermark predicate**
# MAGIC (`last_modified_timestamp > watermark`), stamps lineage metadata on every
# MAGIC row, and appends to Bronze Delta partitioned by `_batch_id`.
# MAGIC
# MAGIC **Bronze stores every column as STRING on purpose.** A cast that fails
# MAGIC during ingestion destroys the evidence — `"N/A"` cast to INT is just NULL,
# MAGIC and nobody can then tell what the source actually sent. Keeping the raw
# MAGIC text is what lets Silver quarantine a row with its original value attached.
# MAGIC
# MAGIC The notebook is a thin wrapper: all logic lives in `src/bronze/ingest.py`
# MAGIC so it is unit-tested and runs identically on a laptop and on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the project code
# MAGIC
# MAGIC When ADF runs this notebook as a job, the `retailintel` wheel is attached as
# MAGIC a **cluster library** and `src` / `config` import automatically. Running the
# MAGIC notebook interactively in the workspace UI attaches no libraries, so the
# MAGIC same import fails with `ModuleNotFoundError: No module named 'src'`.
# MAGIC
# MAGIC This cell makes the notebook self-sufficient in both cases. `%pip install`
# MAGIC is a no-op if the wheel is already present, and `restartPython()` is what
# MAGIC makes the newly installed package visible to the current session.

# COMMAND ----------

# MAGIC %pip install /Workspace/Shared/RetailIntel/retailintel-0.1.0-py3-none-any.whl

# COMMAND ----------

# Restarts the Python interpreter so the freshly installed wheel is importable.
# Widget values survive this — they are notebook state, not Python state — so the
# parameter cells below still read correctly.
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("batch_id", "batch_01_initial", "Batch to ingest")
dbutils.widgets.text("run_id", "", "ADF pipeline run id")
dbutils.widgets.text("full_reload", "false", "Ignore watermarks")
dbutils.widgets.text(
    "lakehouse_root",
    "abfss://lakehouse@stretailintel26c66f.dfs.core.windows.net",
    "ADLS lakehouse root",
)
# Where ADF's Copy activity landed this batch. Defaults to <lakehouse>/raw,
# which is where pl_retailintel_medallion writes it.
dbutils.widgets.text("landing_root", "", "Landing root (blank = <lakehouse>/raw)")

batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")
full_reload = dbutils.widgets.get("full_reload").lower() == "true"
lakehouse_root = dbutils.widgets.get("lakehouse_root")
landing_root = dbutils.widgets.get("landing_root")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment
# MAGIC
# MAGIC `RETAILINTEL_ENV=databricks` makes `config/project_config.py` resolve every
# MAGIC path to `abfss://` instead of the local filesystem — the same code, a
# MAGIC different root. Storage is reached through the workspace's managed identity,
# MAGIC so there is no key or token anywhere in this notebook.

# COMMAND ----------

import os
import sys

os.environ["RETAILINTEL_ENV"] = "databricks"
os.environ["RETAILINTEL_LAKEHOUSE_ROOT"] = lakehouse_root
if landing_root:
    os.environ["RETAILINTEL_LANDING_ROOT"] = landing_root

# The project code is installed on the cluster as the `retailintel` wheel
# (see the activity's `libraries`), so `src` and `config` import normally.
# This path is a fallback for running the notebook against a Repos checkout.
REPO_PATH = "/Workspace/Repos/RetailIntel"
if os.path.isdir(REPO_PATH) and REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage access
# MAGIC
# MAGIC When ADF runs this notebook, the job cluster is created with these OAuth
# MAGIC settings already in its Spark config. An **interactive** cluster has none of
# MAGIC them, so the very first ADLS read fails with
# MAGIC `AZURE_INVALID_CREDENTIALS_CONFIGURATION` — Spark falls back to looking for
# MAGIC an account key that deliberately does not exist.
# MAGIC
# MAGIC Setting them at session level here makes the notebook work on *any* cluster.
# MAGIC Re-setting them on a job cluster is harmless.
# MAGIC
# MAGIC The credentials come from the `retailintel` secret scope — never from code.
# MAGIC `dbutils.secrets.get` returns a value Databricks redacts from all output, so
# MAGIC the secret cannot leak into a notebook result or a screenshot.

# COMMAND ----------

# Derive the storage host from the lakehouse root rather than hardcoding it, so
# this works unchanged against a different storage account.
_sa_host = lakehouse_root.split("@")[1].split("/")[0]
_tenant = dbutils.secrets.get("retailintel", "tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{_sa_host}", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{_sa_host}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{_sa_host}",
    dbutils.secrets.get("retailintel", "sp-client-id"),
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{_sa_host}",
    dbutils.secrets.get("retailintel", "sp-client-secret"),
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{_sa_host}",
    f"https://login.microsoftonline.com/{_tenant}/oauth2/token",
)
print(f"ADLS OAuth configured for {_sa_host}")

# COMMAND ----------

from src.bronze.ingest import ingest_batch  # noqa: E402
from src.common.control import new_run_id  # noqa: E402

effective_run_id = run_id or new_run_id()
print(f"batch_id={batch_id}  run_id={effective_run_id}  full_reload={full_reload}")
print(f"lakehouse_root={lakehouse_root}")

# COMMAND ----------

# MAGIC %md ## Ingest
# MAGIC
# MAGIC A missing or renamed column raises `SchemaValidationError` and fails the
# MAGIC notebook — and therefore the ADF activity. That is deliberate: a broken data
# MAGIC contract has no sensible partial result, unlike a single bad row.

# COMMAND ----------

results = ingest_batch(batch_id, effective_run_id, full_reload=full_reload)

# COMMAND ----------

# MAGIC %md ## Result

# COMMAND ----------

import json  # noqa: E402

total_ingested = sum(r["rows_ingested"] for r in results)
total_filtered = sum(r.get("rows_filtered_by_watermark", 0) for r in results)

print(f"rows ingested            : {total_ingested:,}")
print(f"rows filtered (watermark): {total_filtered:,}")
for r in results:
    print(f"  {r['source']:<12} {r['rows_ingested']:>8,}  watermark→{r['watermark_to']}")

# Returned to ADF so the run outcome is visible in the monitor without opening
# the notebook.
dbutils.notebook.exit(
    json.dumps(
        {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "run_id": effective_run_id,
            "rows_ingested": total_ingested,
            "rows_filtered_by_watermark": total_filtered,
            "sources": results,
        },
        default=str,
    )
)
