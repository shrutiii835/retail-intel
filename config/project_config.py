"""
RetailIntel — central configuration.

One module holds every path, constant and tunable so the *same* code runs in two
places without edits:

  * LOCAL      — laptop, Spark in local[*] mode, lakehouse on the filesystem
  * DATABRICKS — Azure Databricks, lakehouse on ADLS Gen2 via abfss://

Which one is active is decided by the RETAILINTEL_ENV environment variable
(default "local"). Nothing here contains a secret; ADLS access is granted to the
Databricks workspace identity, not through keys in code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------

ENV = os.environ.get("RETAILINTEL_ENV", "local").lower()

REPO_ROOT = Path(__file__).resolve().parents[1]

# Local filesystem locations (also used as the ADF "on-premises" source system)
DATA_DIR = REPO_ROOT / "data"
SOURCE_DIR = DATA_DIR / "raw" / "source"      # simulated source systems (CSV)
LANDING_DIR = DATA_DIR / "landing"            # per-batch extracts ADF picks up
SAMPLE_DIR = DATA_DIR / "sample"              # small committed samples
METRICS_DIR = REPO_ROOT / "metrics"           # measured results (JSON/MD)

# Lakehouse root — Delta tables live here
if ENV == "databricks":
    # Set by the Databricks job / notebook widget; e.g.
    #   abfss://lakehouse@stretailintel.dfs.core.windows.net
    LAKEHOUSE_ROOT = os.environ["RETAILINTEL_LAKEHOUSE_ROOT"]
else:
    LAKEHOUSE_ROOT = str(REPO_ROOT / "lakehouse")

# Where the ingestion layer reads its batch files from. Locally that is the
# generated landing folder; on Databricks it is the ADLS path the ADF Copy
# activity writes to (lakehouse/raw/<batch_id>/<source>/). Kept as a plain
# string rather than a Path because abfss:// URLs are not filesystem paths —
# Path() would mangle the double slash.
if ENV == "databricks":
    LANDING_ROOT = os.environ.get(
        "RETAILINTEL_LANDING_ROOT", f"{LAKEHOUSE_ROOT}/raw"
    )
else:
    LANDING_ROOT = str(REPO_ROOT / "data" / "landing")

BRONZE_ROOT = f"{LAKEHOUSE_ROOT}/bronze"
SILVER_ROOT = f"{LAKEHOUSE_ROOT}/silver"
GOLD_ROOT = f"{LAKEHOUSE_ROOT}/gold"
QUARANTINE_ROOT = f"{LAKEHOUSE_ROOT}/quarantine"
CONTROL_ROOT = f"{LAKEHOUSE_ROOT}/_control"   # watermarks + run audit log

# --------------------------------------------------------------------------
# Table paths
# --------------------------------------------------------------------------

BRONZE_TABLES = {
    "sales": f"{BRONZE_ROOT}/sales",
    "inventory": f"{BRONZE_ROOT}/inventory",
    "campaigns": f"{BRONZE_ROOT}/campaigns",
    "products": f"{BRONZE_ROOT}/products",
    "stores": f"{BRONZE_ROOT}/stores",
}

SILVER_TABLES = {
    "sales": f"{SILVER_ROOT}/sales",
    "inventory": f"{SILVER_ROOT}/inventory",
    "campaigns": f"{SILVER_ROOT}/campaigns",
    "products": f"{SILVER_ROOT}/products",
    "stores": f"{SILVER_ROOT}/stores",
}

QUARANTINE_TABLES = {
    "sales": f"{QUARANTINE_ROOT}/sales",
    "inventory": f"{QUARANTINE_ROOT}/inventory",
    "campaigns": f"{QUARANTINE_ROOT}/campaigns",
    "products": f"{QUARANTINE_ROOT}/products",
    "stores": f"{QUARANTINE_ROOT}/stores",
}

GOLD_TABLES = {
    "dim_date": f"{GOLD_ROOT}/dim_date",
    "dim_product": f"{GOLD_ROOT}/dim_product",
    "dim_store": f"{GOLD_ROOT}/dim_store",
    "dim_campaign": f"{GOLD_ROOT}/dim_campaign",
    "fact_sales": f"{GOLD_ROOT}/fact_sales",
    "fact_inventory_snapshot": f"{GOLD_ROOT}/fact_inventory_snapshot",
}

WATERMARK_TABLE = f"{CONTROL_ROOT}/watermarks"
RUN_LOG_TABLE = f"{CONTROL_ROOT}/pipeline_runs"
DQ_RESULTS_TABLE = f"{CONTROL_ROOT}/dq_results"

# --------------------------------------------------------------------------
# Business / generation constants
# --------------------------------------------------------------------------

# The resume window. All transactions fall inside it.
PERIOD_START = "2025-01-01"
PERIOD_END = "2025-04-30"

# Date dimension is generated a little wider than the fact data so that
# campaign start/end dates and inventory snapshots always resolve.
DIM_DATE_START = "2024-12-01"
DIM_DATE_END = "2025-05-31"

N_SALES_TRANSACTIONS = 132_000   # inside the required 120K–150K band
N_PRODUCTS = 220
N_STORES = 28
N_CAMPAIGNS = 36
INVENTORY_SNAPSHOT_DAY = 6       # Sunday=6 → weekly snapshots (Mon=0)

RANDOM_SEED = 20250413           # every run reproduces byte-identical data

# Deliberate, *controlled* data-quality defects injected into the raw feeds.
# Kept small on purpose: the point is to prove the pipeline handles bad data,
# not to destroy the dataset. Rates are fractions of the sales row count.
DQ_INJECTION_RATES = {
    "duplicate_rows": 0.0120,        # exact re-sends of an existing transaction
    "null_product_id": 0.0020,
    "null_store_id": 0.0015,
    "null_timestamp": 0.0010,
    "invalid_quantity": 0.0035,      # zero or negative
    "invalid_unit_price": 0.0025,    # negative or non-numeric text
    "orphan_product_id": 0.0020,     # product not present in the product master
    "orphan_store_id": 0.0015,       # store not present in the store master
    "orphan_campaign_id": 0.0015,    # campaign not present in the campaign master
    "type_inconsistency": 0.0020,    # e.g. quantity "3.0" / "N/A", price "1,299.00"
}

# --------------------------------------------------------------------------
# Ingestion batches — how the incremental story is simulated
# --------------------------------------------------------------------------
# The source system carries `last_modified_timestamp`. ADF/the ingestion module
# pulls rows where last_modified_timestamp > watermark, so the batch boundaries
# below are *only* used to generate believable arrival patterns; the pipeline
# itself never reads them.


@dataclass(frozen=True)
class IngestionBatch:
    batch_id: str
    label: str
    modified_through: str   # inclusive upper bound on last_modified_timestamp
    description: str


INGESTION_BATCHES: list[IngestionBatch] = [
    IngestionBatch(
        batch_id="batch_01_initial",
        label="Initial full load",
        modified_through="2025-03-31 23:59:59",
        description="History from 2025-01-01 to 2025-03-31. Watermark starts empty.",
    ),
    IngestionBatch(
        batch_id="batch_02_incremental",
        label="Incremental — new + late corrections",
        modified_through="2025-04-15 23:59:59",
        description=(
            "New April transactions plus back-dated corrections to already-loaded "
            "March rows (quantity/discount adjustments). Proves MERGE upsert."
        ),
    ),
    IngestionBatch(
        batch_id="batch_03_incremental",
        label="Incremental — new + overlapping re-send",
        modified_through="2025-04-30 23:59:59",
        description=(
            "Remaining April transactions. Deliberately re-sends a slice of "
            "batch 2 unchanged to prove the pipeline is idempotent."
        ),
    ),
]

# --------------------------------------------------------------------------
# Analytics assumptions (documented in METRICS.md)
# --------------------------------------------------------------------------

# Promotion ROI baseline: average daily revenue for the campaign's product, at
# the same stores, over the N days immediately before the campaign started.
BASELINE_WINDOW_DAYS = 28

# Inventory turnover uses COGS, which we can compute exactly because the product
# master carries unit_cost. No proxy needed.
#   turnover = COGS over period / average stock value over period


@dataclass(frozen=True)
class SparkTuning:
    """Small-data settings. 132K rows does not need 200 shuffle partitions."""

    shuffle_partitions: int = 8
    app_name: str = "RetailIntel"
    extra: dict[str, str] = field(default_factory=dict)


SPARK = SparkTuning()
