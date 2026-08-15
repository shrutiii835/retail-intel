# AZURE SCREENSHOTS — evidence checklist

Screenshots are the **only** artefact of this project that cannot be
regenerated. Everything else — data, lakehouse, metrics, exports — rebuilds from
a seeded generator. Capture these before running `AZURE_CLEANUP.md`.

The list is deliberately short. Each item proves a *specific* resume claim that
nothing else can prove; there are no decorative screenshots here.

---

## Before you start — redaction rules

Check every screenshot for these before saving. Blur or crop:

| Must be hidden | Where it shows up |
|---|---|
| **Subscription ID** (`f8b18ff6-…`) | Portal breadcrumbs, Overview blades, resource properties |
| **Tenant ID** (`bd3b5615-…`) | Entra ID pages, some Overview blades |
| **Your email / account name** | Top-right account menu on every page |
| **Any access key or connection string** | Storage → Access keys — **do not open this blade at all** |
| **Databricks PAT / any token** | User settings — do not open |
| **Service principal client secret** | Not visible anywhere; never screenshot Entra → Certificates & secrets |

**Safe to show:** resource names, region, SKU, folder structures, row counts,
pipeline run status, notebook code and output, storage account name (it is not a
credential).

**Simplest approach:** sign out of the portal account menu before capturing, or
crop the top-right corner out of every image.

---

## Screenshot the completed job runs — do not re-run the notebooks

For items #09, #10 and #11, open the **finished job run** in the Databricks UI
rather than re-executing the notebook interactively. Three reasons:

1. **The output is already there.** The job runs rendered every table and print
   statement; the UI keeps them.
2. **Re-running gives worse numbers.** The watermark has already advanced, so
   re-running a processed batch correctly reports *zeros* — technically a fine
   idempotency demo, but useless as a data-quality screenshot.
3. **Interactive clusters differ from job clusters.** A job cluster is created
   with the project wheel attached and ADLS OAuth in its Spark config. A cluster
   you create by hand has neither, which produces
   `ModuleNotFoundError: No module named 'src'` and then
   `AZURE_INVALID_CREDENTIALS_CONFIGURATION`. The notebooks now install the
   wheel and configure storage themselves, so interactive runs *do* work — but
   it costs cluster time for no better screenshot.

Navigate: **Workflows → Job runs → `RetailIntel — medallion (all batches)` →**
pick the run → click the task (`b2_silver`, `b3_gold`, `b3_bronze`).

> If you do run interactively, use a **single-user** (Dedicated) cluster. Unity
> Catalog *shared* clusters block session-level `fs.azure.*` credential configs,
> so storage access will fail there regardless.

---

## The checklist

Save into a local `screenshots/` folder (git-ignored — do not commit images of
your portal).

---

### 01 — Resource group with all four resources

- **Service:** Resource groups
- **Page:** Portal → Resource groups → `rg-retailintel` → **Overview**
- **Must be visible:** all resources listed — `stretailintel26c66f` (Storage
  account), `adf-retailintel` (Data factory), `dbw-retailintel` (Azure
  Databricks Service) — plus the region `Central India`
- **Redact:** subscription ID, your email
- **Proves:** the Azure architecture exists as described — ADLS + ADF +
  Databricks, minimum resource set
- **Filename:** `01_resource_group.png`

---

### 02 — ADLS Gen2 with hierarchical namespace enabled

- **Service:** Storage account
- **Page:** `stretailintel26c66f` → **Settings → Configuration**
- **Must be visible:** **Hierarchical namespace: Enabled**, and Account kind
  `StorageV2`
- **Redact:** subscription ID, your email
- **Proves:** this is genuinely ADLS Gen2, not plain Blob storage — the single
  most common thing candidates get wrong when they claim "ADLS Gen2"
- **Filename:** `02_adls_hns_enabled.png`

---

### 03 — Medallion folder structure in the lakehouse container

- **Service:** Storage account
- **Page:** `stretailintel26c66f` → **Data storage → Containers → `lakehouse`**
- **Must be visible:** the folders `bronze/`, `silver/`, `gold/`,
  `quarantine/`, `_control/`, `raw/`
- **Redact:** nothing beyond the standard account/subscription redactions
- **Proves:** the Bronze/Silver/Gold medallion architecture physically exists on
  ADLS, plus the quarantine and control layers
- **Filename:** `03_medallion_structure.png`

---

### 04 — Gold star schema tables

- **Service:** Storage account
- **Page:** Containers → `lakehouse` → **`gold/`**
- **Must be visible:** `dim_date/`, `dim_product/`, `dim_store/`,
  `dim_campaign/`, `fact_sales/`, `fact_inventory_snapshot/`,
  `mart_campaign_roi/`
- **Proves:** the star schema named on the resume (FactSales, DimProduct,
  DimStore, DimDate, DimCampaign) exists as real Delta tables
- **Filename:** `04_gold_star_schema.png`

---

### 05 — Delta transaction log

- **Service:** Storage account
- **Page:** Containers → `lakehouse` → `gold/fact_sales/` → **`_delta_log/`**
- **Must be visible:** the `.json` commit files (`00000000000000000000.json`,
  `…0001.json`, …) alongside `.parquet` data files in the parent folder
- **Proves:** Delta Lake is genuinely in use — the transaction log is what
  separates Delta from plain Parquet, and it is the concrete answer to "how do
  you know it's Delta?"
- **Filename:** `05_delta_transaction_log.png`

---

### 06 — ADF pipeline canvas

- **Service:** Data Factory
- **Page:** `adf-retailintel` → **Launch Studio** → Author → Pipelines →
  `pl_retailintel_medallion`
- **Must be visible:** the activity chain — `SetSourceList` →
  `CopyEachSourceToRaw` (ForEach) → `BronzeIngest` → `SilverBuild` → `GoldBuild`
- **Proves:** ADF is used for real orchestration, and shows the ADF-orchestrates
  / Databricks-transforms boundary
- **Filename:** `06_adf_pipeline.png`

---

### 07 — ADF successful run

- **Service:** Data Factory Studio
- **Page:** **Monitor → Pipeline runs**
- **Must be visible:** `pl_retailintel_medallion` runs with status **Succeeded**,
  and their durations. Capture with **all three batch runs** visible if possible.
- **Proves:** the pipeline actually executed successfully in Azure — not just
  that it was authored
- **Filename:** `07_adf_run_succeeded.png`

---

### 08 — ADF activity-level run detail

- **Service:** Data Factory Studio
- **Page:** Monitor → Pipeline runs → click a run → **activity list**
- **Must be visible:** each activity Succeeded, with duration. Hover the Copy
  activity to show **rows read / rows written** if the tooltip is available.
- **Proves:** orchestration ordering and dependency behaviour; the Databricks
  activities being invoked by ADF
- **Filename:** `08_adf_activity_detail.png`

---

### 09 — Databricks notebook run output (Silver, data quality)

- **Service:** Databricks
- **Page:** Workspace → `/Shared/RetailIntel/02_silver_build` → a completed run,
  scrolled to the **data-quality summary** output
- **Must be visible:** the printed rows read / quarantined / duplicates
  collapsed / updated-by-MERGE figures, and ideally the per-rule DQ table
- **Redact:** the workspace URL contains no secret, but crop your account name
- **Proves:** schema validation, duplicate detection and quarantine all ran on
  Azure Databricks — three explicit resume claims in one image
- **Filename:** `09_databricks_silver_dq.png`

---

### 10 — Databricks notebook run output (Gold, star schema verification)

- **Service:** Databricks
- **Page:** `/Shared/RetailIntel/03_gold_build`, scrolled to the verification
  cell and the monthly revenue display
- **Must be visible:** `fact_sales rows`, `PK unique: True`,
  `NULL foreign keys: 0`, and the campaign ROI table showing `roi` and
  `roi_margin_based`
- **Proves:** the star schema was built on Azure, its primary key is unique
  (dedup + MERGE worked), referential integrity holds, and the ROI KPI was
  computed
- **Filename:** `10_databricks_gold_verification.png`

---

### 11 — Incremental load evidence

- **Service:** Databricks
- **Page:** The **Bronze** notebook output from an *incremental* batch run
  (`batch_02_incremental` or `batch_03_incremental`)
- **Must be visible:** `rows filtered (watermark)` greater than zero, and the
  per-source `watermark→` values
- **Proves:** incremental loading with a watermark is real and working — the
  rows filtered out are ones already processed. This is the hardest claim to
  prove and the easiest to fake, so it deserves its own screenshot.
- **Filename:** `11_incremental_watermark.png`

---

### 12 — Power BI dashboard

- **Service:** Power BI Desktop (local, not Azure)
- **Page:** `RetailIntel.pbix` → **Executive Overview** page, and separately the
  **Campaign Analysis** page
- **Must be visible:**
  - Overview: Total Revenue, Units Sold, Portfolio ROI, Inventory Turnover cards
    plus the revenue trend chart
  - Campaign: the ROI table with both `roi` and margin-based ROI, showing the
    profitable/unprofitable split
- **Proves:** the dashboards tracking promotion ROI, inventory turnover and
  store performance exist
- **Filenames:** `12a_powerbi_overview.png`, `12b_powerbi_campaigns.png`

---

## Optional — only if you want one extra

### 13 — Quarantine contents

- **Page:** Databricks Silver notebook, the quarantine sample table output
- **Must be visible:** rows with original raw values and `_dq_failed_rules_csv`
  like `SAL_006,SAL_010`
- **Proves:** invalid records are quarantined *with reasons*, not silently
  dropped. Strong supporting evidence for the data-quality claim, but §09
  already covers the headline.
- **Filename:** `13_quarantine_rows.png`

---

## Coverage check

| Resume claim | Screenshot |
|---|---|
| ADLS Gen2 | 02, 03 |
| Azure Data Factory | 06, 07, 08 |
| Databricks + PySpark | 09, 10, 11 |
| Bronze / Silver / Gold | 03 |
| Delta Lake | 05 |
| Star schema (5 tables) | 04, 10 |
| Incremental loading | 11 |
| Schema validation | 09 |
| Duplicate detection | 09, 10 (`PK unique: True`) |
| 120K+ records | 10 (`fact_sales rows`) |
| >99% consistency | 09 + `metrics/data_quality_report.json` |
| ~30% query improvement | `metrics/query_benchmark.json` (no screenshot needed) |
| Power BI dashboards | 12a, 12b |
| Promotion ROI | 10, 12b |
| Inventory turnover | 12a |
| Store performance | 12 (Store Performance page) |

**12 images** (13 with the optional one). Nothing redundant — each proves
something the others cannot.
