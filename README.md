# RetailIntel — Enterprise Retail Promotion Intelligence Lakehouse

An end-to-end Lakehouse pipeline that integrates three retail data sources
(sales, inventory, campaigns) through a Bronze → Silver → Gold medallion
architecture on Delta Lake, and serves a star schema to Power BI for promotion
ROI, inventory turnover and store performance analysis.

> **This is an enterprise-inspired mini-project, not a production platform.**
> The data is synthetic, the business users are simulated, and the impact
> figures are measured against a simulated baseline. Every claim in this repo is
> backed by a number that was actually measured — see [METRICS.md](METRICS.md),
> including the claims that did *not* hold up.

---

## Architecture

```
   SOURCE SYSTEMS              INGESTION           TRANSFORMATION        SERVING
 ┌──────────────────┐      ┌───────────────┐    ┌────────────────┐   ┌───────────┐
 │ sales.csv        │      │ Azure Data    │    │ Azure          │   │ Power BI  │
 │ inventory.csv    │─────▶│ Factory       │───▶│ Databricks     │──▶│ Desktop   │
 │ campaigns.csv    │      │ (Copy +       │    │ (PySpark)      │   │           │
 │ products.csv     │      │  watermark)   │    │                │   │ 4 pages   │
 │ stores.csv       │      └───────────────┘    └────────────────┘   └───────────┘
 └──────────────────┘              │                     │
                                   ▼                     ▼
                          ┌────────────────────────────────────────┐
                          │            ADLS Gen2 / Delta Lake      │
                          │  bronze/ ──▶ silver/ ──▶ gold/         │
                          │     raw       validated    star schema │
                          │     strings   deduped      + ROI mart  │
                          │              quarantine/   _control/   │
                          └────────────────────────────────────────┘
```

Full detail, including failure modes and layer responsibilities, is in
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## What it actually does

| Capability | Where |
|---|---|
| Watermark-based incremental ingestion | `src/bronze/ingest.py` |
| Schema contract validation (structural) | `src/common/schemas.py` |
| 40 declarative row-level quality rules | `src/common/schemas.py` |
| Quarantine of invalid rows with reasons | `src/silver/build_silver.py` |
| Duplicate detection on business keys | `src/silver/build_silver.py` |
| Delta MERGE upsert (late corrections) | `src/silver/build_silver.py`, `src/gold/build_gold.py` |
| Star schema with surrogate keys | `src/gold/build_gold.py` |
| Promotion ROI with pre-period baseline | `src/gold/kpis.py` |
| Post-load consistency assertions | `src/quality/consistency_report.py` |
| Idempotency proof (replays a batch) | `src/quality/idempotency_check.py` |
| Query performance benchmark | `src/benchmark/query_benchmark.py` |

---

## Headline numbers (all measured, not estimated)

| Metric | Result | Evidence |
|---|---|---|
| Transactions curated into Gold | **130,023** | `metrics/data_quality_report.json` |
| Source rows processed | 135,375 | `metrics/last_pipeline_run.json` |
| Curated data consistency | **99.90%** | 24 post-load assertions |
| Invalid rows quarantined | 2,002 (1.48%) | `quarantine/sales` |
| Duplicates collapsed | 2,945 | Silver dedup |
| Late corrections applied by MERGE | 405 | Gold audit log |
| Defect detection recall | **100%** on all reject rules | vs injected ground truth |
| Query speed-up vs manual CSV workflow | **65–75%**, significant on 6/6 queries | `metrics/query_benchmark.json` |
| Pipeline idempotent on replay | **Yes — zero rows changed** | `metrics/idempotency_check.json` |
| Tests | **59 passing** | `pytest tests/` |

Two claims did **not** survive measurement and are documented as such in
[METRICS.md](METRICS.md): the star-schema-only performance isolation (inside
measurement noise), and the ~40% manual-effort reduction (a simulated estimate,
not an observed result).

---

## Verified on Azure

The pipeline was not only written for Azure — it was **run** there, and the
results were compared figure by figure against the local run.

| | Local | **Azure** |
|---|---:|---:|
| `fact_sales` rows | 130,023 | **130,023** |
| Primary key unique | ✓ | **✓** |
| NULL foreign keys | 0 | **0** |
| Quarantined | 2,002 | **2,002** |
| Duplicates collapsed | 2,945 | **2,945** |
| MERGE updates | 405 | **405** |
| Watermark-filtered (batch 3) | 1,752 | **1,752** |
| Portfolio ROI | 0.4158 | **0.4158** |

**Every figure identical.** That is what `RETAILINTEL_ENV` buys — one codebase,
two storage roots, no forked logic.

- **Databricks:** multi-task job, 7 tasks on a shared single-node cluster, all
  SUCCESS. Bronze→Silver→Gold across all three batches.
- **ADF:** `pl_retailintel_medallion` Succeeded — `SetSourceList` →
  `CopyEachSourceToRaw` (ForEach, 5 copies) → `BronzeIngest` → `SilverBuild` →
  `GoldBuild`.
- **Idempotency, proven through the full orchestration:** that ADF run replayed
  an already-processed batch. Bronze ingested **0 rows** (watermark filtered
  everything), Silver read **0**, and Gold still reported 130,023 rows with a
  unique PK and zero null foreign keys.

Two things broke on Azure that never break locally, both fixed and documented
rather than hidden — see [DECISIONS.md](DECISIONS.md) D15 and D16:

1. **`Standard_DS3_v2` capacity stockout** in Central India. Not quota —
   capacity. The runner now falls through a list of six 4-core node types.
2. **`OPTIMIZE delta.\`path\`` SQL failed** with
   `UC_HIVE_METASTORE_DISABLED_EXCEPTION`. That syntax resolves through
   `spark_catalog` → Hive metastore, which Unity Catalog workspaces disable. The
   Python `DeltaTable.forPath(...).optimize()` API works in both environments.

---

## Quick start

```bash
# 1. Environment (Python 3.12 + JDK 17 required for Spark)
/opt/homebrew/bin/python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# 2. Generate the synthetic source data (~132K transactions, seeded)
.venv/bin/python -m src.data_generation.generate_data

# 3. Run the full pipeline across all three batches
.venv/bin/python -m src.pipeline --batch all --full-reload

# 4. Build the promotion ROI mart
.venv/bin/python -m src.gold.kpis

# 5. Verify everything
.venv/bin/python -m src.quality.consistency_report
.venv/bin/python -m src.quality.idempotency_check
.venv/bin/python -m src.benchmark.query_benchmark
.venv/bin/python -m pytest tests/ -q

# 6. Export for Power BI
.venv/bin/python -m src.gold.export_powerbi
```

Run a single incremental batch:

```bash
.venv/bin/python -m src.pipeline --batch batch_02_incremental
```

---

## Repository layout

```
RetailIntel/
├── README.md              you are here
├── CONTEXT.md             project memory: scope, status, decisions, limits
├── ARCHITECTURE.md        components, data flow, failure modes
├── DECISIONS.md           every significant choice, with alternatives
├── DATA_DICTIONARY.md     field-by-field definitions
├── DATA_QUALITY.md        rules, quarantine, measured results
├── METRICS.md             every resume claim vs measured evidence
├── STUDY_GUIDE.md         teaches the whole project from zero
├── AZURE_SCREENSHOTS.md   exact evidence checklist
├── AZURE_CLEANUP.md       what to shut down, in what order
│
├── config/project_config.py     all paths and constants
├── src/
│   ├── data_generation/   synthetic source data
│   ├── common/            Spark session, watermarks, schemas, audit
│   ├── bronze/            raw ingestion
│   ├── silver/            validate → quarantine → dedupe → MERGE
│   ├── gold/              star schema, KPI mart, Power BI export
│   ├── quality/           consistency + idempotency verification
│   ├── benchmark/         query performance measurement
│   └── pipeline.py        orchestrator
├── scripts/
│   ├── deploy_adf.py      render + deploy the ADF definitions
│   └── dbx_run.py         submit notebooks to Databricks as job runs
├── notebooks/             Databricks notebook versions of each layer
├── adf/                   Data Factory pipeline definitions
├── tests/                 59 tests
├── powerbi/               DAX measures, build guide, exported data
└── metrics/               measured results (JSON)
```

---

## Where to start reading

1. [README.md](README.md) — this file
2. [ARCHITECTURE.md](ARCHITECTURE.md) — how the pieces fit
3. [STUDY_GUIDE.md](STUDY_GUIDE.md) — every concept from zero, with interview questions
4. [DECISIONS.md](DECISIONS.md) — why each technology, with the alternatives rejected
5. [DATA_QUALITY.md](DATA_QUALITY.md) — the rules and what they caught
6. [METRICS.md](METRICS.md) — the claims and the evidence

---

## Limitations

Stated plainly, because being able to name them is the difference between
understanding a project and reciting it:

- Data is **synthetic** and generated by `src/data_generation/generate_data.py`.
- Business users are **simulated**; nobody used these dashboards to make a decision.
- Volume is **130K rows** — enough to be realistic, far below a volume that
  would stress Spark. Scaling behaviour is reasoned about, not demonstrated.
- The promotion baseline is a **simple pre-period average**. It does not
  separate genuine incremental demand from pull-forward, and does not net out
  seasonality. A real read needs control stores.
- Dimensions are **SCD Type 1** — no attribute history is retained.
- No streaming, no CI/CD, no production deployment, no orchestration beyond ADF.
- Security is proportional to a mini-project: no secrets in code, Azure identity
  based access, but no enterprise governance layer.

