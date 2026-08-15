# DATA QUALITY

## 1. Two kinds of failure, kept apart

The single most important distinction in this document:

| | Structural failure | Row-level failure |
|---|---|---|
| **Example** | `store_id` column missing from the feed | `quantity = -3` in one row |
| **Meaning** | The data *contract* is broken | One record is unusable |
| **Detected by** | `validate_structure()` | Declarative rules |
| **Response** | **Raise `SchemaValidationError`, fail the run** | **Quarantine the row, continue** |
| **Why** | There is no sensible partial result when the shape is wrong. A silently renamed column nulls a whole measure and nobody notices for months | Bad data is normal. One malformed record must not block 135,000 good ones |

Getting this backwards is the classic mistake: pipelines that die on one bad row
(brittle), or that silently swallow a renamed column (dangerous).

---

## 2. Rule catalogue

Rules are **declarative** — a rule id, a description, a severity, and a Spark SQL
expression that is TRUE when the row passes. One list drives three things: the
filter that splits clean from quarantined rows, the per-rule counts written to
the `_control/dq_results` table, and this documentation.

Defined in `src/common/schemas.py`.

### Severities

- **REJECT** — the row cannot be trusted. Quarantined; never reaches Silver.
- **REPAIR** — the row is usable once a defined substitution is applied. It
  stays, and the substitution is recorded so its effect stays measurable.

### Sales (`sales`) — business key `transaction_id`

| Rule | Description | Severity | Rows failed |
|---|---|---|---:|
| SAL_001 | `transaction_id` must be present | reject | 0 |
| SAL_002 | `product_id` must be present | reject | 267 |
| SAL_003 | `store_id` must be present | reject | 201 |
| SAL_004 | `transaction_timestamp` must parse to a timestamp | reject | 132 |
| SAL_005 | `quantity` must parse to a whole number | reject | 132 |
| SAL_006 | `quantity` must be greater than zero | reject | 462 |
| SAL_007 | `unit_price` must parse to a number | reject | 0 |
| SAL_008 | `unit_price` must not be negative | reject | 336 |
| SAL_009 | `discount_amount` must not be negative | reject | 0 |
| SAL_010 | discount must not exceed the gross line amount | reject | 700 |
| SAL_011 | transaction date must fall inside the reporting period | reject | 0 |
| SAL_012 | `product_id` must exist in the product master | reject | 538 |
| SAL_013 | `store_id` must exist in the store master | reject | 401 |
| SAL_014 | `campaign_id` must resolve when supplied | **repair** | 202 |

> Counts exceed the number of injected defects because a row can fail several
> rules — a row with `quantity = -2` fails both SAL_006 and SAL_010, since a
> negative line total cannot cover its discount. Rows are counted once in the
> rejection total (2,002) and per-rule here.

### Inventory (`inventory`) — business key `(snapshot_date, product_id, store_id)`

INV_001 `inventory_id` present · INV_002 `snapshot_date` parses · INV_003
`product_id` present · INV_004 `store_id` present · INV_005 `stock_quantity`
parses to a whole number · INV_006 `stock_quantity` ≥ 0 · INV_007 product exists
· INV_008 store exists. **All reject. 0 failures.**

### Campaigns (`campaigns`) — business key `campaign_id`

CAM_001 id present · CAM_002/003 dates parse · **CAM_004 `end_date` ≥
`start_date`** · CAM_005 discount between 0 and 100 · CAM_006 cost ≥ 0 ·
CAM_007 product exists. **All reject. 0 failures.**

### Products / Stores

PRD_001–005 (id and name present, cost and price parse and are non-negative,
**`list_price` ≥ `unit_cost`**) and STR_001–004 (id, name, region present,
`open_date` parses). **All reject. 0 failures.**

---

## 3. Why certain rules exist

**SAL_010 — discount ≤ gross line amount.** A cross-field rule, and the only one
that caught a defect the single-column rules missed. A discount larger than the
line total is arithmetically impossible and produces negative revenue that
quietly reduces every total it rolls into. Note it permits discount *equal* to
the line total: a 100% giveaway is a real promotion.

**SAL_012 / SAL_013 — referential integrity.** Enforced in the pipeline because
object storage has none. Without it, a sale referencing a non-existent product
either vanishes from an inner-joined report or produces a NULL category row that
nobody can explain.

**SAL_014 — repair, not reject.** An unresolvable campaign id must *not* delete
the sale. The transaction happened and its revenue must be counted; only the
attribution is unknown. It is routed to the Unknown campaign member with
`campaign_resolved = false`, and counted as a consistency defect in
[METRICS.md](METRICS.md) so its effect stays visible.

**PRD_005 — `list_price` ≥ `unit_cost`.** Catches a master-data error that would
otherwise produce structurally negative margin on every sale of that product.

---

## 4. Duplicate detection

### What counts as a duplicate

A duplicate is **two rows sharing the same business key**, not two identical
rows. That distinction matters: a corrected transaction and its original are not
identical, but they are the same transaction and only one may survive.

| Feed | Business key |
|---|---|
| sales | `transaction_id` |
| inventory | `(snapshot_date, product_id, store_id)` |
| campaigns / products / stores | `campaign_id` / `product_id` / `store_id` |

Inventory is keyed on the composite grain rather than `inventory_id`, because a
restated snapshot could legitimately arrive with a new surrogate id.

### Detection and resolution

```sql
ROW_NUMBER() OVER (
  PARTITION BY <business key>
  ORDER BY last_modified_timestamp DESC, _ingest_timestamp DESC
) = 1
```

**The newest version wins.** `_ingest_timestamp` is the tie-breaker for exact
re-sends where `last_modified` is identical.

This is deliberately *not* a bare `dropDuplicates()`. `dropDuplicates()` on the
full row would keep both versions of a corrected transaction (they differ), and
`dropDuplicates(["transaction_id"])` keeps an arbitrary one — which could be the
stale version.

### Why dedup runs before the MERGE

Delta's MERGE **errors** if one target row matches multiple source rows; it
cannot decide which wins. Since the source deliberately re-sends transactions, a
batch legitimately contains the same id twice. Collapsing first is what makes
the MERGE deterministic rather than a runtime failure.

### How double counting is prevented — three layers

1. **Watermark** — an already-processed window returns zero rows. Measured:
   1,752 rows correctly filtered in batch 3.
2. **Dedup** — collapses duplicates within a batch. Measured: 2,945 collapsed.
3. **MERGE on business key** — a re-presented row updates rather than inserts.
   Measured: 405 updates.

Proof that all three work together: `fact_sales` contains **130,023 rows and
130,023 distinct `transaction_id`s** (assertion `FS_A15`).

---

## 5. Quarantine

Invalid rows are written to `quarantine/<source>/`, partitioned by
`_dq_batch_id`, carrying:

- every **original raw column value** (uncast — the point is to see what the
  source actually sent)
- `_dq_failed_rules` — array of failed rule ids
- `_dq_failed_rules_csv` — same, readable
- `_dq_run_id`, `_dq_batch_id`, `_quarantined_at`

Example:

| transaction_id | quantity | unit_price | failed rules |
|---|---|---|---|
| TXN00002630 | -2 | 7.25 | `SAL_006,SAL_010` |
| TXN00049315 | N/A | 15.74 | `SAL_005` |

Partitioning by batch makes quarantine **idempotent**: re-running a batch
replaces its partition via `replaceWhere` rather than appending duplicates.

**Known gap.** There is no automated path to reprocess a corrected quarantined
row — it would be re-presented by the source with a newer `last_modified` and
picked up by the normal watermark flow. Quarantine also needs monitoring, or it
accumulates silently; a rising rejection rate is the signal that something
upstream changed.

---

## 6. Measured results

### Volume

| | Rows |
|---|---:|
| Presented to Silver | 135,375 |
| Quarantined | **2,002 (1.48%)** |
| Duplicates collapsed | 2,945 |
| Updated by MERGE | 405 |
| Curated to Silver/Gold | **130,023** |

### Post-load assertions — 23 of 24 passed

All 20 `fact_sales` / `fact_inventory_snapshot` assertions on key resolution,
primary-key uniqueness, measure domains and revenue arithmetic pass, plus all 3
Silver→Gold reconciliation checks (zero variance on row count and revenue).

The single non-passing assertion is `FS_A08_no_unknown_campaign` — the 198 rows
routed to the Unknown campaign member. This is *by design* (SAL_014 is a repair
rule) and those rows are precisely what the consistency percentage counts as
defects. See [METRICS.md](METRICS.md) M2.

### Consistency

**99.9033%** of 204,823 curated rows are fully consistent.

### Detection recall vs injected ground truth

**100% on every reject rule.** The generator records the exact transaction id of
every injected defect, so this is a direct check, not an inference. Full table
in [METRICS.md](METRICS.md) M5.

### Reproduced on Azure

The same three batches were processed on Azure Databricks against ADLS Gen2, and
every data-quality figure came out **identical** to the local run:

| Figure | Local | Azure |
|---|---:|---:|
| Quarantined (3 batches) | 2,002 | **2,002** |
| Duplicates collapsed | 2,945 | **2,945** |
| MERGE updates | 405 | **405** |
| Watermark-filtered, batch 3 | 1,752 | **1,752** |
| `fact_sales` PK unique | true | **true** |
| NULL foreign keys | 0 | **0** |

Worth stating because it is the point of the environment abstraction: the rules
are not re-implemented for the cloud. The same wheel, the same rule list, the
same outcomes.

---

## 7. Controlled defect injection

Defects are injected deliberately at a small rate (**2.9% of source rows**) to
prove the pipeline handles bad data. Types are applied to *disjoint* row sets so
a single row is never doubly-explained and scoring stays unambiguous.

| Type | Injected |
|---|---:|
| Exact duplicate re-sends | 1,584 |
| Invalid quantity (≤ 0) | 462 |
| Invalid unit_price (< 0) | 330 |
| Null product_id | 264 |
| Orphan product_id | 264 |
| Null store_id | 198 |
| Orphan store_id | 198 |
| Orphan campaign_id | 198 |
| Type error — recoverable (`"3.0"`) | 132 |
| Type error — unrecoverable (`"N/A"`) | 132 |
| Null timestamp | 132 |

**Recoverable vs unrecoverable is tracked separately on purpose.** `"3.0"` is a
formatting artefact carrying a valid quantity and the correct response is to
**repair** it; `"N/A"` carries no value and must be **rejected**. Scoring them
together would make correct behaviour look like a 50% miss — which it did, until
the ground truth was split.

### The one interesting edge case

`TXN00074852` was injected as recoverable (`"2.0"`), but its `discount_amount`
still reflected the original larger quantity, so the discount exceeded the new
line total. **SAL_010 rejected it.** The repair rule and the cross-field rule
disagreed and the stricter one won — the correct outcome, and a good
demonstration that cross-field validation catches what column-level validation
cannot.

---

## 8. Where the evidence lives

| Artefact | Path |
|---|---|
| Rule definitions | `src/common/schemas.py` |
| Enforcement | `src/silver/build_silver.py` |
| Per-rule results | `_control/dq_results` (Delta) |
| Run audit log | `_control/pipeline_runs` (Delta) |
| Quarantined rows | `quarantine/<source>/` (Delta) |
| Injected ground truth | `metrics/data_generation_summary.json` |
| Consistency report | `metrics/data_quality_report.json` |
| Idempotency proof | `metrics/idempotency_check.json` |
| Tests | `tests/test_validation_rules.py`, `tests/test_dedup_and_merge.py` |
