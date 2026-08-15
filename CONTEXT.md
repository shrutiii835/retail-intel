# CONTEXT — project memory

The persistent state of the project: what it is, what is built, what is
measured, and what is deliberately not done. Updated whenever the
implementation materially changes.

**Last updated:** project complete. Built and verified locally, deployed to
Azure, and executed end-to-end there via both a Databricks multi-task job and
the ADF pipeline — with every figure matching the local run. Only screenshots,
the `.pbix` build and teardown remain, all of which require the user.

---

## 1. Objective

Build an end-to-end Lakehouse pipeline integrating three retail data sources
(sales, inventory, campaigns) through Bronze → Silver → Gold on Delta Lake,
serving a star schema to Power BI for promotion ROI, inventory turnover and
store performance — small enough to understand completely, complete enough to
defend in an interview.

## 2. Business problem

A retailer runs promotions and cannot tell which ones paid back. The three feeds
that would answer the question arrive separately, disagree with each other, and
are reconciled by hand in Excel every month. Cleaning is redone (differently)
each cycle, corrections create duplicates or get missed, and the reconciliation
is slow enough that campaign decisions are made before the analysis lands.

## 3. Scope

**In scope:** synthetic source data with controlled defects · watermark-based
incremental ingestion · schema contract + row-level validation · quarantine ·
duplicate detection · Delta MERGE upsert · star schema · promotion ROI ·
inventory turnover · store performance · Power BI · measured metrics.

**Explicitly out of scope (§5):** CI/CD, Terraform, Docker, Kubernetes, Kafka,
Airflow, Event Hubs, Functions, Synapse, Fabric, ML, GenAI, streaming,
microservices, complex networking or governance.

## 4. Architecture

```
sources (CSV) → ADF (Copy + orchestrate) → ADLS Gen2 / Delta
                                              bronze → silver → gold
                                              quarantine/  _control/
                                                    ↓
                                          Databricks + PySpark
                                                    ↓
                                              Power BI Desktop
```

`ADF = ingestion + orchestration. Databricks = transformation.`
Full detail in [ARCHITECTURE.md](ARCHITECTURE.md).

## 5. Technologies

| Layer | Technology | Version |
|---|---|---|
| Storage | ADLS Gen2 (HNS) | Standard LRS Hot |
| Ingestion | Azure Data Factory | — |
| Compute | Azure Databricks | DBR 15.4 LTS |
| Processing | PySpark | 3.5.3 |
| Table format | Delta Lake | 3.2.1 |
| Reporting | Power BI Desktop | — |
| Local dev | Python 3.12 + JDK 17 | — |

Local and Databricks run **the same code**, switched by `RETAILINTEL_ENV`.

## 6. Datasets

| Feed | Rows | Grain |
|---|---:|---|
| sales | 133,584 written (132,000 clean + 1,584 dup) | one transaction line |
| inventory | 74,800 | product × store × week |
| campaigns | 36 | one campaign |
| products | 220 | one product |
| stores | 28 | one store |

Period 2025-01-01 → 2025-04-30. Seeded (`RANDOM_SEED = 20250413`) — reruns
reproduce byte-identical data.

Three ingestion batches simulate incremental arrival: initial load, then two
incrementals containing new transactions, back-dated corrections, and a
deliberate re-send to prove idempotency.

## 7. Implementation status

| Component | Status |
|---|---|
| Repo structure, config | ✅ |
| Synthetic data generator | ✅ |
| Bronze ingestion + watermark | ✅ verified |
| Silver validation / quarantine / dedup / MERGE | ✅ verified |
| Gold star schema + surrogate keys | ✅ verified |
| Promotion ROI mart | ✅ verified |
| Consistency report (24 assertions) | ✅ 99.90% |
| Idempotency proof | ✅ zero drift |
| Query benchmark | ✅ 65–75%, 6/6 significant |
| Test suite | ✅ 59 passing |
| Power BI semantic model (PBIP) | ✅ generated — 7 tables, 8 relationships, 40 measures |
| Power BI report visuals | ⏳ needs Windows — Power BI Desktop has no macOS build |
| ADF pipeline / linked services / datasets | ✅ deployed and **run successfully** |
| Databricks notebooks | ✅ **run successfully in Azure**, all 3 batches |
| Documentation | ✅ all 10 files complete |
| Azure resources | ✅ created and verified |
| **Screenshots** | ✅ captured — 14 images covering every Azure claim |
| **Cleanup** | ✅ done — all Azure resources and the service principal deleted |

### Azure execution evidence

**Databricks multi-task job** `RetailIntel — medallion (all batches)`
(job `239749031004429`, run `81223830415868`) — 7 tasks on one shared
single-node cluster, **all SUCCESS**. Combined with the earlier standalone
Bronze run, all three batches went through all three layers on Azure.

Final Azure state, **identical to local in every figure**:

| Metric | Value |
|---|---:|
| `fact_sales` rows | 130,023 |
| Primary key unique | true |
| NULL foreign keys | 0 |
| Quarantined (3 batches) | 2,002 |
| Duplicates collapsed | 2,945 |
| MERGE updates | 405 |
| Watermark-filtered, batch 3 | 1,752 |
| Portfolio ROI | 0.4158 |

**ADF pipeline** `pl_retailintel_medallion`
(run `722cc0d0-9759-11f1-8f35-22a1cc599287`) — **Succeeded**, all activities:

| Activity | Status | Duration |
|---|---|---:|
| SetSourceList | Succeeded | 0.3s |
| CopyEachSourceToRaw (ForEach → 5 copies) | Succeeded | 94s |
| BronzeIngest | Succeeded | 424s |
| SilverBuild | Succeeded | 214s |
| GoldBuild | Succeeded | 262s |

That run replayed an already-processed batch, which makes it a live
**idempotency proof through the full orchestration**: Bronze ingested 0 rows
(watermark filtered everything), Silver read 0, and Gold still reported 130,023
rows with a unique primary key and zero null foreign keys.

## 8. Measured results

| Metric | Value |
|---|---:|
| Source rows processed | 135,375 |
| Curated into `fact_sales` | 130,023 |
| `fact_inventory_snapshot` | 74,800 |
| Quarantined | 2,002 (1.48%) |
| Duplicates collapsed | 2,945 |
| Watermark-filtered (batch 3 re-send) | 1,752 |
| MERGE updates (late corrections) | 405 |
| **Curated consistency** | **99.9033%** |
| Defect detection recall | 100% on all reject rules |
| Net revenue | ₹1,957,583.32 |
| Gross margin | 30.56% |
| Inventory turnover | 3.17 turns (4 months) |
| Campaigns / profitable | 36 / 20 |
| **Portfolio ROI (revenue)** | **+0.42** |
| **Portfolio ROI (margin)** | **−1.00** |
| Query improvement vs manual CSV | **65–75%** (6/6 significant) |
| Idempotency | zero rows changed on replay |

## 9. Key decisions

Full reasoning in [DECISIONS.md](DECISIONS.md). The load-bearing ones:

- **Bronze stores everything as STRING** — a failed cast destroys the evidence
  needed to explain a rejection.
- **Watermark on `last_modified_timestamp`**, per layer, advanced only after a
  successful write. Not CDC, and documented as not-CDC.
- **Dedup before MERGE** — Delta MERGE errors on multiple source matches.
- **Quarantine, don't drop or fail** — but a *missing column* fails the run.
- **Unknown dimension members** so facts join INNER without losing rows.
- **Revenue defined once, in Silver.**
- **SCD Type 1** — no attribute history.
- **CSV export for Power BI** — avoids a billing SQL warehouse.

## 10. Assumptions

- All stores participate in every campaign (no store-level targeting).
- One campaign promotes exactly one product.
- Campaign-period performance is measured on the promoted **product over the
  campaign date window**, not on rows tagged with the campaign id.
- The ROI baseline is the 28 days immediately preceding the campaign.
- Stock is valued at cost, matching COGS in the turnover ratio.
- Weekly inventory snapshots are representative of the whole week.

## 11. Limitations

- Synthetic data; defect rates chosen, not observed.
- Simulated business users — no dashboard drove a real decision.
- 130K rows is too small to demonstrate Spark's advantages; Pandas would be
  faster at this size.
- **The star-schema-only performance isolation is inside measurement noise and
  is not claimed.**
- **The ~40% effort reduction is a simulated estimate, not an observed result.**
- Promotion baseline does not separate incremental demand from pull-forward,
  and does not net out seasonality.
- Timestamp-based incremental loading **cannot detect deletes**.
- SCD Type 1 loses attribute history.
- Security is proportional to a mini-project: no Unity Catalog, no private
  endpoints, no column masking.

## 12. Azure resources

✅ **Created and verified.** Subscription `Azure subscription 1`
(`f8b18ff6-…`), **Free Trial**, spending limit **ON**, region **centralindia**.
Signed-in identity holds **Owner**, which is what made managed-identity role
assignments possible.

| Resource | Name | Purpose | Configuration | Status |
|---|---|---|---|---|
| Resource Group | `rg-retailintel` | Container for all resources | centralindia | ✅ Created |
| Storage (ADLS Gen2) | `stretailintel26c66f` | Lakehouse + landing | Standard_LRS, StorageV2, **HNS enabled**, TLS 1.2, public blob access disabled | ✅ Created |
| Containers | `landing`, `lakehouse` | Source drop / medallion layers | — | ✅ Created, 33 blobs uploaded |
| Data Factory | `adf-retailintel` | Ingestion + orchestration | v2, system-assigned MI `681f11e1-…`, **no triggers** | ✅ Created |
| Databricks | `dbw-retailintel` | Transformation | **trial** SKU (14-day free DBUs), `adb-7405607439445982.2.azuredatabricks.net` | ✅ Created |
| Service principal | `sp-retailintel-databricks` | Databricks → ADLS OAuth | appId `bc839281-…`, scoped to the storage account only | ✅ Created |

**Role assignments** (least privilege — all scoped to a single resource, never
the subscription):

| Principal | Role | Scope |
|---|---|---|
| Signed-in user | Storage Blob Data Contributor | storage account |
| ADF managed identity | Storage Blob Data Contributor | storage account |
| ADF managed identity | Contributor | Databricks workspace |
| `sp-retailintel-databricks` | Storage Blob Data Contributor | storage account |

**No secrets are recorded in this file or anywhere in the repository.** The
service principal's client secret was written directly into the Databricks
secret scope `retailintel` and never displayed. The cluster Spark config carries
only `{{secrets/retailintel/...}}` references, which Databricks resolves at
runtime.

### Deviations from the original plan, and why

- **Databricks → ADLS uses a service principal, not a managed identity.**
  Managed-identity access to ADLS requires Unity Catalog external locations, and
  provisioning a UC metastore is an account-level UI operation on a new trial
  workspace. The SP + secret-scope pattern is the standard pre-UC approach. Less
  clean than MI; recorded here rather than glossed over.
- **Node type is `Standard_D4ads_v5`, not `Standard_DS3_v2`.** CentralIndia
  returned `CLOUD_PROVIDER_RESOURCE_STOCKOUT` for DS3_v2 on this subscription.
  Quota was never the problem (every family allows 4 vCPUs) — it was capacity.
  `scripts/dbx_run.py` now falls through a candidate list rather than
  hardcoding one size.
- **DBFS root is disabled** on new workspaces, so the project wheel is stored as
  a workspace file at `/Workspace/Shared/RetailIntel/` and installed as a
  cluster library.

## 13. Screenshot status

⏳ **Pending — user action.** Implementation is complete and verified, so the
checklist in `AZURE_SCREENSHOTS.md` is now final: 12 screenshots, each mapped to
a specific resume claim, with explicit redaction rules. Azure resources must
stay up until these are captured.

## 14. Cleanup status

✅ **Complete.** All Azure resources deleted after screenshots were captured:

- Resource group `rg-retailintel` deleted (storage account
  `stretailintel26c66f`, Data Factory `adf-retailintel`, Databricks workspace
  `dbw-retailintel`), along with the Databricks-managed resource group
- Service principal and app registration `sp-retailintel-databricks` deleted
  from Entra ID — these live outside the resource group and would otherwise
  have been orphaned with a live credential
- All job clusters had already self-terminated; no compute was left running

**Ongoing Azure cost: zero.**

Nothing of value was lost. Everything except the screenshots regenerates from
the seeded generator:

    python -m src.data_generation.generate_data
    python -m src.pipeline --batch all --full-reload
    python -m src.gold.kpis && python -m src.gold.export_powerbi

## 15. Remaining work

Everything that can be done without the user is done. What is left:

1. **Capture screenshots** per `AZURE_SCREENSHOTS.md` — requires the Azure
   Portal UI, which cannot be scripted.
2. **Build the Power BI report** per `powerbi/README.md`. **Requires Windows** —
   Power BI Desktop has no macOS build, and `.pbix` embeds a compiled tabular
   database that only the Analysis Services engine can write, so it cannot be
   generated by a script on any platform.

   Mitigated as far as possible: `scripts/build_pbip.py` generates a **PBIP
   project** with the complete semantic model — 7 tables with 94 explicitly
   typed columns, 8 relationships, 40 DAX measures, `dim_date` already marked as
   the date table. Opening it on Windows leaves only visual placement (~20 min).
   Report pages are generated empty on purpose: visual-container JSON cannot be
   validated without Power BI Desktop, and a malformed report definition stops
   the whole project opening.
3. **Run `AZURE_CLEANUP.md`** once screenshots are captured.
