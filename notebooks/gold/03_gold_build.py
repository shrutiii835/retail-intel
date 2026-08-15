# Databricks notebook source
# MAGIC %md
# MAGIC # RetailIntel — Gold build (star schema + ROI mart)
# MAGIC
# MAGIC Invoked by the ADF activity `GoldBuild`.
# MAGIC
# MAGIC ```
# MAGIC         dim_date ────┐
# MAGIC       dim_product ───┼──▶  fact_sales  ◀─── dim_campaign
# MAGIC         dim_store ───┘          │
# MAGIC               └──▶ fact_inventory_snapshot
# MAGIC         dim_campaign ──▶ mart_campaign_roi
# MAGIC ```
# MAGIC
# MAGIC **Grain**
# MAGIC - `fact_sales` — one sales transaction. PK `transaction_id` (degenerate dimension).
# MAGIC - `fact_inventory_snapshot` — one product × store × weekly snapshot date.
# MAGIC   A *periodic snapshot* fact whose measure is **semi-additive**: sum across
# MAGIC   products and stores, never across dates.
# MAGIC
# MAGIC Every dimension carries an **Unknown** member (`-1`), and `dim_campaign` also
# MAGIC carries **No Campaign** (`0`). That is what lets the fact be joined with an
# MAGIC INNER join and still lose nothing — NULL foreign keys would silently drop
# MAGIC rows from inner-joined reports.

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
from src.gold.build_gold import build_gold  # noqa: E402
from src.gold.kpis import build_campaign_roi_mart  # noqa: E402

effective_run_id = run_id or new_run_id()
print(f"batch_id={batch_id}  run_id={effective_run_id}  full_reload={full_reload}")

# COMMAND ----------

# MAGIC %md ## Dimensions and facts
# MAGIC
# MAGIC Dimensions are SCD Type 1 with a change-detection hash, so an unchanged row
# MAGIC is not rewritten and the "rows updated" figure stays meaningful. Facts are
# MAGIC MERGEd on their business key, so a late correction updates in place instead
# MAGIC of being counted twice.
# MAGIC
# MAGIC The run finishes with `OPTIMIZE ... ZORDER`, which compacts the small files
# MAGIC every incremental MERGE leaves behind.

# COMMAND ----------

results = build_gold(batch_id, effective_run_id, full_reload=full_reload)

# COMMAND ----------

# MAGIC %md ## Promotion ROI mart
# MAGIC
# MAGIC Rebuilt in full each run — it is 36 rows, and its baseline window spans
# MAGIC dates outside whatever batch just landed, so an incremental build would be
# MAGIC more complex for no benefit.

# COMMAND ----------

roi = build_campaign_roi_mart()
for k, v in roi.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md ## Verify the star schema
# MAGIC
# MAGIC Two checks worth running every time: the fact's primary key must be unique
# MAGIC (proof that dedup and MERGE worked), and no foreign key may be NULL.

# COMMAND ----------

from config.project_config import GOLD_TABLES  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
total = fact.count()
distinct = fact.select("transaction_id").distinct().count()
null_fks = fact.where(
    "product_key IS NULL OR store_key IS NULL OR campaign_key IS NULL OR date_key IS NULL"
).count()

print(f"fact_sales rows          : {total:,}")
print(f"distinct transaction_id  : {distinct:,}")
print(f"PK unique                : {total == distinct}")
print(f"NULL foreign keys        : {null_fks}")

assert total == distinct, "fact_sales primary key is not unique — dedup or MERGE failed"
assert null_fks == 0, "fact_sales has NULL foreign keys — Unknown-member fallback failed"

# COMMAND ----------

display(
    fact.groupBy("year_month")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("quantity").alias("units"),
    )
    .orderBy("year_month")
)

# COMMAND ----------

# MAGIC %md ## Campaign ROI — revenue basis vs margin basis
# MAGIC
# MAGIC These two disagree sharply, and that is the point. On revenue the portfolio
# MAGIC looks profitable; on margin the discount given away cancels the gross profit
# MAGIC from the extra volume. A revenue-only ROI is the number that gets a campaign
# MAGIC renewed; the margin ROI is the number that should decide it.

# COMMAND ----------

from src.gold.kpis import MART_CAMPAIGN_ROI  # noqa: E402

display(
    spark.read.format("delta")
    .load(MART_CAMPAIGN_ROI)
    .select("campaign_id", "campaign_name", "category", "duration_days",
            "discount_percentage", "campaign_cost", "campaign_revenue",
            "incremental_revenue", "roi", "roi_margin_based", "uplift_pct")
    .orderBy(F.col("roi").desc())
)

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "run_id": effective_run_id,
            "fact_sales_rows": total,
            "pk_unique": total == distinct,
            "null_foreign_keys": null_fks,
            "campaign_roi": roi,
        },
        default=str,
    )
)
