# METRICS

Every numerical claim, how it was measured, and what the measurement actually
returned — including the two claims that did **not** hold up. Nothing here was
tuned to hit a target; where the measurement disagrees with the resume, the
measurement wins and the disagreement is stated.

**Reproduce everything:**

```bash
.venv/bin/python -m src.data_generation.generate_data
.venv/bin/python -m src.pipeline --batch all --full-reload
.venv/bin/python -m src.gold.kpis
.venv/bin/python -m src.quality.consistency_report      # → metrics/data_quality_report.json
.venv/bin/python -m src.quality.idempotency_check       # → metrics/idempotency_check.json
.venv/bin/python -m src.benchmark.query_benchmark       # → metrics/query_benchmark.json
```

---

## Summary

| Resume claim | Status | Measured |
|---|---|---|
| 3 retail data sources | ✅ Supported | sales, inventory, campaigns (+2 masters) |
| 120K+ transaction records | ✅ Supported | **135,375 processed → 130,023 curated** |
| >99% data consistency | ✅ Supported | **99.90%** across 24 assertions |
| ~30% query performance improvement | ✅ Supported, conservatively | **65–75%** vs the manual workflow |
| ~40% manual reporting effort reduction | ⚠️ **Estimate, not measured** | Simulated time-and-motion only |
| Star-schema-only speed-up | ❌ **Not supported** | Inside measurement noise |

---

## M1 — Volume: "120K+ retail transaction records"

**Definition.** Transaction rows passing through the pipeline and landing in the
curated Gold fact table.

**Method.** Row counts from the pipeline audit log (`_control/pipeline_runs`)
and `COUNT(*)` on `gold/fact_sales`.

| Stage | Rows |
|---|---:|
| Clean transactions generated | 132,000 |
| Source rows written (incl. 1,584 injected duplicates) | 133,584 |
| Source rows presented across 3 batches (incl. 1,791 prior versions) | 137,127 |
| Ingested to Bronze (after watermark filtering) | **135,375** |
| Filtered by watermark as already-processed | 1,752 |
| Quarantined as invalid | 2,002 |
| Duplicates collapsed | 2,945 |
| Updated in place by MERGE | 405 |
| **Curated into `fact_sales`** | **130,023** |
| `fact_inventory_snapshot` | 74,800 |

**Result.** ✅ 130,023 curated transactions, 135,375 processed. Both exceed 120K.

**Evidence.** `metrics/data_generation_summary.json`,
`metrics/last_pipeline_run.json`, `metrics/data_quality_report.json`.

---

## M2 — Consistency: ">99% data consistency across curated datasets"

This is the claim most easily faked, so the definition is deliberately strict
and the three things it could mean are kept apart.

### Definition used

> **Curated consistency** = the share of rows in the Gold layer that pass
> *every* post-load assertion: unique primary key, every foreign key resolvable
> to a **real** (non-Unknown) dimension member, and every measure inside its
> declared domain.

Note what this deliberately does **not** do: it does not count quarantined
source rows as inconsistency. A rejected row is the pipeline working correctly.
Folding rejections into this number would let a dirtier source make the figure
look worse, or — worse — let a pipeline that silently drops bad rows score 100%.

### Result

| | |
|---|---:|
| Curated rows evaluated (`fact_sales` + `fact_inventory_snapshot`) | 204,823 |
| Defect rows | 198 |
| **Consistency** | **99.9033%** |
| Assertions passed | 23 / 24 |

✅ **Claim supported: 99.90% > 99%.**

### What the 198 defect rows are

All 198 are `FS_A08_no_unknown_campaign`: sales rows whose source `campaign_id`
did not exist in the campaign master. They were **repaired, not rejected** —
the sale genuinely happened and its revenue must count, so it is routed to the
Unknown campaign member (key `-1`) with `campaign_resolved = false`.

They are counted as consistency defects anyway, because their campaign
attribution *is* degraded. Excluding them would have produced a suspiciously
perfect 100.00%, which would say less, not more. **Campaign attribution
completeness is 99.85%** (129,825 of 130,023 rows attributed).

### Reconciliation — Silver vs Gold

Internal consistency is worthless if rows were lost in a join, so this is
checked separately:

| Check | Silver | Gold | Variance |
|---|---:|---:|---:|
| Sales row count | 130,023 | 130,023 | **0** |
| Sales net revenue | 1,957,583.32 | 1,957,583.32 | **0.00** |
| Inventory row count | 74,800 | 74,800 | **0** |

**Evidence.** `metrics/data_quality_report.json`.

---

## M3 — Query performance: "~30% improvement"

### Method

Six representative analytical queries — four full-scan aggregations and two
filtered (a month + region slice, and a single-campaign drilldown) — because a
real Power BI page almost always has a slicer applied, and partition pruning
only pays off when there is a predicate to prune on.

Each query runs in three variants: **raw CSV** (the pre-project manual
workflow), **Silver Delta** (clean data, string keys, revenue computed at query
time), and **Gold star schema** (integer surrogate keys, stored revenue, monthly
partitions, `OPTIMIZE ... ZORDER`).

1 warm-up run (discarded) + **15 measured runs** per variant.
`spark.catalog.clearCache()` before every single run. **Median** reported —
at these durations one GC pause skews a mean badly.

**Result equivalence is verified**, because a speed-up between two queries that
return different answers is meaningless. Silver and Gold agree exactly. The raw
CSV baseline differs by exactly 4 rows / 7 units / ₹39.71, and that difference
is *fully explained*: those 4 transactions had their current source version
quarantined, so the incremental pipeline correctly retains the last known-good
version while a fresh recomputation from CSV drops them. Unexplained variance:
**0 rows**.

### Result

Latest authoritative run (`metrics/query_benchmark.json`):

| Query | Raw CSV | Silver Delta | Gold star | vs CSV |
|---|---:|---:|---:|---:|
| Q1 store performance | 0.603s | 0.320s | 0.262s | 56.5% |
| Q2 sales by category | 0.546s | 0.259s | 0.296s | 45.8% |
| Q3 campaign performance | 0.674s | 0.299s | 0.145s | 78.5% |
| Q4 monthly by region | 0.566s | 0.303s | 0.189s | 66.7% |
| Q5 filtered month + region | 0.656s | 0.307s | 0.181s | 72.4% |
| Q6 single-campaign drilldown | 0.493s | 0.148s | 0.164s | 66.8% |
| **Total** | **3.538s** | **1.635s** | **1.236s** | **65.1%** |

✅ **Claim supported.** Measured **65.1%** reduction in total query time against
the manual CSV workflow — the true before/after of the project. Significant on
**6 of 6** queries, where "significant" means the difference in medians exceeds
the *sum* of the two standard deviations.

**Across five independent benchmark invocations** the CSV→Gold total-time
improvement ranged **65.1% – 76.2%** (the three six-query runs: 65.1%, 69.9%,
72.7%). The figure moves several points between runs because the absolute
durations are sub-second and machine load varies — so the honest statement is
*"roughly 65–75%, consistently significant"*, not a single decimal.

The resume's "~30%" is therefore **conservative** against every run, which is
the safe direction.

### ❌ What did NOT hold up

The **star-schema-only** comparison (Silver Delta → Gold, holding file format
constant) is **not a reportable result**. Across five independent benchmark
invocations it produced 7.3%, 16.3%, 24.4%, 33.3% and 34.6% — a 5x spread — and
the significance test returns **0 of 6 queries** significant in every run.

The reason is structural: at 130K rows on single-node Spark, per-query latency
is dominated by fixed job-setup overhead, and run-to-run standard deviation
(0.06–0.16s) is as large as the effect being measured (~0.1s). The star schema
is very likely faster; this dataset is simply too small to prove it.

**If asked in an interview:** the honest answer is that the ~65–75% figure is
real and reproducible, that it bundles the file-format change together with the
dimensional model, and that isolating the model alone would need a dataset large
enough to escape the noise floor.

**Evidence.** `metrics/query_benchmark.json` (includes per-run timings, standard
deviations and significance flags).

---

## M4 — Manual reporting effort: "~40% reduction"

### ⚠️ This is a simulated estimate, not an observed measurement.

Nobody was timed. No analyst used this system. The figure comes from a
task-by-task comparison of the manual workflow against the automated one, and
it is presented as an **estimate of avoided effort**, not as realised business
impact. Presenting it otherwise would be dishonest.

### Before — manual monthly reporting cycle

| Task | Est. time |
|---|---:|
| Export 3 source extracts | 20 min |
| Clean and type-correct in Excel | 90 min |
| Find and remove duplicate transactions | 45 min |
| Join sales to product/store/campaign masters | 60 min |
| Compute revenue, ROI, turnover by hand | 75 min |
| Build and format the report | 60 min |
| Reconcile and correct errors | 40 min |
| **Total** | **~6.5 hours/month** |

### After — automated pipeline

| Task | Est. time |
|---|---:|
| Trigger ADF pipeline (or wait for schedule) | 5 min |
| Review DQ scorecard and quarantine counts | 20 min |
| Refresh Power BI | 10 min |
| Investigate anything the DQ report flagged | 30 min |
| Commentary and distribution | 60 min |
| **Total** | **~2 hours/month** |

**Estimated reduction: ~69%** on this basis — well above the ~40% claimed, so
the resume figure is again conservative.

### Why the claim is still worth making

The automated steps are the *repeatable* ones. Cleaning, deduplication and
joining are done once in code and re-executed for free; what remains is the
judgement work that should not be automated anyway.

### What would make this a real measurement

Time an analyst producing the report both ways, several cycles each, same
person, same data. That is the only honest way to get this number, and it was
not done.

---

## M5 — Data quality detection recall

The generator records the exact `transaction_id` of every defect it injects, so
detection is scored against ground truth rather than assumed.

| Defect type | Injected | Detected | Recall |
|---|---:|---:|---:|
| null product_id | 264 | 264 | **100%** |
| null store_id | 198 | 198 | **100%** |
| null timestamp | 132 | 132 | **100%** |
| invalid quantity (≤ 0) | 462 | 462 | **100%** |
| invalid unit_price (< 0) | 330 | 330 | **100%** |
| orphan product_id | 264 | 264 | **100%** |
| orphan store_id | 198 | 198 | **100%** |
| type error, unrecoverable (`"N/A"`) | 132 | 132 | **100%** |
| type error, recoverable (`"3.0"`) | 132 | 131 | 99.24% |
| orphan campaign_id (repaired) | 198 | 198 | **100%** |

**The one "miss" is not a miss.** `TXN00074852` had its quantity rewritten to
`"2.0"` while its `discount_amount` still reflected the original larger
quantity, so the discount exceeded the line total. Rule `SAL_010` (a cross-field
check) correctly rejected it as internally inconsistent. The repair rule and the
cross-field rule disagreed, and the stricter one won — which is the desired
behaviour.

**Evidence.** `metrics/data_quality_report.json` →
`detection_recall_vs_injected_ground_truth`.

---

## M6 — Idempotency

**Method.** Snapshot every curated table, replay an already-processed batch
end-to-end, snapshot again, and diff.

**Result.** ✅ **Zero differences.** Row counts, revenue totals and unit totals
identical across all seven curated tables.

| Table | Before | After |
|---|---|---|
| `silver_sales` | 130,023 rows / ₹1,957,583.32 | identical |
| `gold_fact_sales` | 130,023 rows / ₹1,957,583.32 | identical |
| `gold_fact_inventory` | 74,800 rows | identical |
| dimensions | 221 / 29 / 38 | identical |

The replay ingested **0 rows** — the watermark filtered the entire
already-processed window before any work was done.

**Evidence.** `metrics/idempotency_check.json`.

---

## M7 — Business KPIs

### Revenue

`net_revenue = quantity × unit_price − discount_amount`, computed once in Silver
(D12) and carried forward unchanged.

| Measure | Value |
|---|---:|
| Gross sales | ₹2,021,224.00 |
| Discounts | ₹63,640.68 (3.15%) |
| **Net revenue** | **₹1,957,583.32** |
| COGS | ₹1,359,394.38 |
| Gross profit | ₹598,188.94 |
| Gross margin | 30.56% |
| Units | 261,000 |
| Transactions | 130,023 |

### Inventory turnover

`turnover = COGS ÷ average inventory value`, **both valued at cost**. No proxy
was needed — the product master carries `unit_cost`, so COGS is exact.

Average inventory averages *across* the 17 weekly snapshot dates and sums
*within* each one, because stock is semi-additive: summing four weekly snapshots
of the same 1,000 units would claim 4,000 units that never existed.

| | |
|---|---:|
| COGS (4 months) | ₹1,359,394.38 |
| Average inventory at cost | ₹429,436.04 |
| **Inventory turnover** | **3.17 turns** |
| Annualised | ~9.5 turns |

### Promotion ROI — and the finding that matters

```
baseline_daily_revenue    = revenue for the promoted product over the 28 days before the campaign ÷ 28
expected_baseline_revenue = baseline_daily_revenue × campaign duration
incremental_revenue       = actual campaign-period revenue − expected_baseline_revenue
roi                       = (incremental_revenue − campaign_cost) ÷ campaign_cost
```

| | |
|---|---:|
| Campaigns | 36 |
| Profitable (revenue basis) | 20 (56%) |
| Total campaign cost | ₹77,992.68 |
| Total incremental revenue | ₹110,425.49 |
| **Portfolio ROI (revenue basis)** | **+0.42** |
| Average campaign ROI | +0.37 |
| Total incremental **gross profit** | **−₹269.62** |
| **Portfolio ROI (margin basis)** | **−1.00** |

**The two ROIs disagree completely, and that is the most interesting result in
the project.** On revenue the promotion portfolio looks like a 42% return. On
margin it destroys value: the discount given away almost exactly cancels the
gross profit from the extra volume sold.

This is a genuine and very common retail phenomenon — promotions reliably move
units and routinely fail to pay for themselves once the margin sacrifice is
counted. A revenue-only ROI is the number that gets a campaign renewed; the
margin ROI is the number that should decide it. Both are exposed in the mart and
on the dashboard so the gap is visible rather than hidden.

**Assumptions and limits** (also in D11): the baseline does not separate
incremental demand from pull-forward; seasonality inside the campaign window is
attributed to the campaign; all stores are assumed to participate. A defensible
read needs control stores.

---

## M8 — Test coverage

**59 tests, all passing** (`pytest tests/ -q`).

| File | Tests | Covers |
|---|---:|---|
| `test_data_generation.py` | 12 | Referential integrity, campaign window validity, price sanity, defect rate, watermark ordering, and that promotions actually lift volume |
| `test_validation_rules.py` | 22 | Every rule individually; blank vs orphan campaign; recoverable vs fractional quantity; structural contract failure |
| `test_dedup_and_merge.py` | 12 | Dedup keeps newest; MERGE updates rather than appends; stale replay cannot revert a correction; watermark predicate |
| `test_kpi_calculations.py` | 13 | Revenue, COGS, ROI (incl. break-even and total-loss cases), semi-additivity of stock, divide-by-zero guards |

---

## M9 — Azure execution: does the cloud run agree with the laptop?

The whole point of `RETAILINTEL_ENV` is that the same code runs in both places.
That is only a claim until the two are compared, so here is the comparison.

The full medallion ran on Azure Databricks (DBR 15.4 LTS, single-node
`Standard_D4ads_v5`) across all three batches, reading landing files from ADLS
Gen2 and writing Bronze/Silver/Gold Delta tables back to ADLS.

| Metric | Local | **Azure** | Match |
|---|---:|---:|:--:|
| `fact_sales` rows | 130,023 | **130,023** | ✅ |
| Primary key unique | true | **true** | ✅ |
| NULL foreign keys | 0 | **0** | ✅ |
| Rows quarantined (3 batches) | 2,002 | **2,002** | ✅ |
| Duplicates collapsed | 2,945 | **2,945** | ✅ |
| MERGE updates (late corrections) | 405 | **405** | ✅ |
| Watermark-filtered rows, batch 3 | 1,752 | **1,752** | ✅ |
| Campaigns / profitable | 36 / 20 | **36 / 20** | ✅ |
| Total campaign cost | ₹77,992.68 | **₹77,992.68** | ✅ |
| Total incremental revenue | ₹110,425.49 | **₹110,425.49** | ✅ |
| Portfolio ROI | 0.4158 | **0.4158** | ✅ |

**Every figure is identical.** That is expected — the generator is seeded and
the logic is the same wheel — but it is worth verifying rather than assuming,
because an environment difference (timezone, decimal handling, file ordering)
would show up here first.

### Incremental behaviour, observed in Azure

The per-batch outputs show the watermark doing real work rather than being
decorative:

| Batch | Sales rows available | Ingested | Filtered by watermark |
|---|---:|---:|---:|
| `batch_01_initial` | 99,908 | 99,908 | 0 |
| `batch_02_incremental` | 17,529 | 17,529 | 0 |
| `batch_03_incremental` | 19,690 | **17,938** | **1,752** |

Batch 3 deliberately re-sends part of batch 2. The watermark rejected exactly
those 1,752 rows before any processing happened. Reference data
(products/stores/campaigns) returned `SUCCESS_NO_DATA` on batches 2 and 3 —
unchanged masters, so nothing to re-ingest.

Silver reported **218 MERGE updates** on batch 2 and **187** on batch 3: the
late corrections updating already-loaded transactions in place rather than
duplicating them.

### The ROI mart converging as data arrives

A nice side effect of running batch by batch — the ROI is rebuilt each time, on
progressively more complete data:

| After batch | Profitable campaigns | Portfolio ROI |
|---|---:|---:|
| 1 | 15 / 36 | −0.12 |
| 2 | 18 / 36 | +0.25 |
| 3 | **20 / 36** | **+0.42** |

Campaigns running late in the period only look profitable once their sales have
actually landed. This is a useful thing to be able to explain: a partially
loaded fact table produces a *confidently wrong* ROI, not an obviously broken
one.

### What ran where

| Component | Evidence |
|---|---|
| ADLS Gen2 | `bronze/`, `silver/`, `gold/`, `quarantine/`, `_control/` in the `lakehouse` container; HNS enabled |
| Delta Lake | `_delta_log/` on every table; `fact_sales` partitioned `year_month=202501..202504` |
| Databricks | Multi-task job `RetailIntel — medallion (all batches)`, 8 tasks, shared single-node job cluster |
| ADF | `pl_retailintel_medallion` — Copy ForEach + 3 Databricks notebook activities |

---

## M10 — Resume-defence audit

Every claim in the resume line, mapped to the thing that proves it and the
sentence to say when asked.

| Resume claim | Evidence | Where | How measured | Say this |
|---|---|---|---|---|
| **3 retail data sources** | sales, inventory, campaigns (+ product & store masters) | `data/raw/source/`, `src/data_generation/generate_data.py` | Row counts per feed | "Three transactional feeds plus two masters. Sales is the fact, inventory is a weekly snapshot, campaigns is the promotion master." |
| **Azure Data Factory** | `pl_retailintel_medallion`: ForEach + Copy + 3 Databricks activities | `adf/`, Portal Monitor | Pipeline run succeeded | "ADF orchestrates and moves data. It never transforms — that boundary keeps the logic testable in Python." |
| **ADLS Gen2** | Storage account with **HNS enabled**, containers `landing` + `lakehouse` | Portal → Configuration | `isHnsEnabled: true` | "Gen2 specifically, because the hierarchical namespace makes directory operations atomic, which Delta depends on." |
| **Databricks** | Single-node job cluster running 3 notebooks | Databricks workspace, run history | Run status SUCCESS | "Job clusters, not interactive — created per run and terminated when it ends, so nothing is left billing." |
| **PySpark** | All transformation logic | `src/bronze/`, `src/silver/`, `src/gold/` | 59 tests pass | "The notebooks are thin wrappers; the logic is a packaged wheel so the same code runs locally and on the cluster." |
| **Bronze / Silver / Gold** | Three physical layers + quarantine + control | `lakehouse/` container | Folder structure | "Bronze preserves raw strings, Silver validates once for everyone, Gold models for questions." |
| **Delta Lake** | `_delta_log` on every table; MERGE, OPTIMIZE, ZORDER used | `gold/fact_sales/_delta_log/` | Commit files present | "Delta is Parquet plus a transaction log. I use ACID, MERGE, schema enforcement and OPTIMIZE — all four." |
| **Star schema (5 tables)** | DimDate, DimProduct, DimStore, DimCampaign, FactSales (+ inventory fact, ROI mart) | `gold/` | Table counts: 182/221/29/38/130,023 | "Grain is one row per sales transaction. Integer surrogate keys, and every dimension has an Unknown member so facts join INNER without losing rows." |
| **Incremental loading** | Watermark per layer in `_control/watermarks` | Audit log | **1,752 rows filtered**, 405 MERGE updates | "Watermark on `last_modified_timestamp`, advanced only after a successful write. Not CDC — it can't detect deletes, and I don't claim it can." |
| **Schema validation** | Structural contract + 40 declarative rules | `src/common/schemas.py` | 2,002 quarantined | "A missing column fails the run; a bad row is quarantined. Those are different failures and must be treated differently." |
| **Duplicate detection** | Dedup on business key, newest wins | `src/silver/build_silver.py` | **2,945 collapsed**; PK unique assertion passes | "A duplicate is two rows sharing a business key, not two identical rows. Dedup runs before MERGE because MERGE errors on multiple source matches." |
| **120K+ transactions** | 135,375 processed → **130,023 curated** | `metrics/data_quality_report.json` | `COUNT(*)` on `fact_sales` | "130,023 in the curated fact table, 135,375 processed including rejects and duplicates." |
| **>99% consistency** | **99.9033%** across 24 assertions | `metrics/data_quality_report.json` | Defect rows ÷ curated rows | "Measured on curated data: unique PK, all FKs resolving to real members, measures in domain. Rejected source rows are counted separately — they're the pipeline working." |
| **~30% query improvement** | **65–75%**, significant on 6/6 | `metrics/query_benchmark.json` | 15 runs, median, cache cleared, results reconciled | "65–75% against the manual CSV workflow, consistent across five runs. I also isolated the star schema alone and that came out inside measurement noise, so I don't claim it." |
| **~40% effort reduction** | ⚠️ **Simulated estimate** | METRICS.md M4 | Task-by-task comparison | "That one is an estimate from a task-level comparison, not an observed measurement. Nobody was timed." |
| **Power BI dashboards** | 4 pages, 40+ DAX measures | `powerbi/` | Totals reconcile to Gold | "Executive, Campaign, Store, Inventory. The measures handle stock as semi-additive — averaged across dates, never summed." |
| **Promotion ROI** | `mart_campaign_roi`, 36 campaigns | `gold/mart_campaign_roi` | 28-day pre-period baseline | "+0.42 on revenue but −1.00 on margin. The discount cancels the gross profit from the extra volume." |
| **Inventory turnover** | **3.17 turns** | DAX + `fact_inventory_snapshot` | COGS ÷ avg inventory, both at cost | "Both sides valued at cost, otherwise the ratio is meaningless." |
| **Store performance** | Store page + `Store Rank by Revenue` | `powerbi/measures.dax` | 28 stores ranked | "Revenue, units, basket value and rank, sliceable by region and format." |

---

## Honest limitations on all of the above

- Data is **synthetic**; defect rates are chosen, not observed.
- Benchmarks run on **one laptop, single-node Spark**. Cloud results will differ.
- The 130K-row volume is **too small to demonstrate** Spark's real advantages;
  at this size Pandas would be faster.
- The ~40% effort reduction is **simulated** (M4).
- No production deployment, no real users, no observed business impact.
