# Databricks notebook source
# MAGIC %md
# MAGIC # RetailIntel — Silver build
# MAGIC
# MAGIC Invoked by the ADF activity `SilverBuild`.
# MAGIC
# MAGIC ```
# MAGIC read Bronze above the Silver watermark
# MAGIC   → try_cast to real types          (failures become NULL, not exceptions)
# MAGIC   → resolve referential integrity
# MAGIC   → evaluate every rule             (one boolean column per rule)
# MAGIC   → split: REJECT failures → quarantine
# MAGIC   → apply REPAIR substitutions
# MAGIC   → deduplicate on the business key (newest last_modified wins)
# MAGIC   → MERGE into Silver Delta
# MAGIC   → write per-rule DQ counts
# MAGIC   → advance the watermark           (only now)
# MAGIC ```
# MAGIC
# MAGIC **Deduplication runs before the MERGE** because Delta's MERGE errors when
# MAGIC one target row matches multiple source rows — and the source deliberately
# MAGIC re-sends transactions.
# MAGIC
# MAGIC **The watermark advances last.** If the write fails it stays put and the
# MAGIC next run re-reads the same window, which is safe because every write is a
# MAGIC MERGE on a business key.

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

dbutils.widgets.text("batch_id", "batch_01_initial", "Batch")
dbutils.widgets.text("run_id", "", "ADF pipeline run id")
dbutils.widgets.text("full_reload", "false", "Ignore watermarks")
dbutils.widgets.text(
    "lakehouse_root",
    "abfss://lakehouse@stretailintel26c66f.dfs.core.windows.net",
    "ADLS lakehouse root",
)

batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")
full_reload = dbutils.widgets.get("full_reload").lower() == "true"
lakehouse_root = dbutils.widgets.get("lakehouse_root")

# COMMAND ----------

import os
import sys

os.environ["RETAILINTEL_ENV"] = "databricks"
os.environ["RETAILINTEL_LAKEHOUSE_ROOT"] = lakehouse_root

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

from src.common.control import new_run_id  # noqa: E402
from src.silver.build_silver import build_silver  # noqa: E402

effective_run_id = run_id or new_run_id()
print(f"batch_id={batch_id}  run_id={effective_run_id}  full_reload={full_reload}")

# COMMAND ----------

results = build_silver(batch_id, effective_run_id, full_reload=full_reload)

# COMMAND ----------

# MAGIC %md ## Data quality summary
# MAGIC
# MAGIC Rejected rows are **not** a pipeline failure — they are the pipeline doing
# MAGIC its job. They are counted, quarantined with their failure reasons, and the
# MAGIC run continues.

# COMMAND ----------

from config.project_config import DQ_RESULTS_TABLE, QUARANTINE_TABLES  # noqa: E402

read = sum(r["rows_read"] for r in results)
rejected = sum(r["rows_rejected"] for r in results)
dupes = sum(r["duplicates_removed"] for r in results)
inserted = sum(r["inserted"] for r in results)
updated = sum(r["updated"] for r in results)

print(f"rows read           : {read:,}")
print(f"quarantined         : {rejected:,} ({100 * rejected / read if read else 0:.3f}%)")
print(f"duplicates collapsed: {dupes:,}")
print(f"inserted            : {inserted:,}")
print(f"updated by MERGE    : {updated:,}")

# COMMAND ----------

# Per-rule breakdown for this run — the evidence behind DATA_QUALITY.md.
display(
    spark.read.format("delta")
    .load(DQ_RESULTS_TABLE)
    .where(f"run_id = '{effective_run_id}'")
    .select("table_name", "rule_id", "rule_description", "severity",
            "rows_evaluated", "rows_failed", "fail_pct")
    .orderBy("table_name", "rule_id")
)

# COMMAND ----------

# A sample of quarantined rows, with the ORIGINAL raw values and the rules they
# failed. This is what makes a rejection explainable rather than a bare count.
display(
    spark.read.format("delta")
    .load(QUARANTINE_TABLES["sales"])
    .where(f"_dq_batch_id = '{batch_id}'")
    .select("transaction_id", "product_id", "store_id", "quantity", "unit_price",
            "discount_amount", "_dq_failed_rules_csv")
    .limit(25)
)

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "run_id": effective_run_id,
            "rows_read": read,
            "rows_quarantined": rejected,
            "duplicates_collapsed": dupes,
            "rows_inserted": inserted,
            "rows_updated": updated,
        },
        default=str,
    )
)
