# DECISIONS

Every significant choice, the alternatives that were rejected, and what each one
costs. A decision without a stated trade-off is not a decision — it is a
preference, and it will not survive an interview question.

---

## D1 — ADLS Gen2 as the storage layer

**Context.** The lakehouse needs durable storage for three layers of Delta
tables, readable by Spark and cheap at rest.

**Chosen.** Azure Data Lake Storage Gen2 (Standard, LRS, Hot) with hierarchical
namespace enabled.

**Why.** The hierarchical namespace is the point: it makes directory rename and
delete atomic metadata operations rather than per-object copies. Delta Lake
depends on cheap directory listing and atomic file operations, and on flat blob
storage those become O(n) and slow. HNS also enables POSIX-style ACLs.

**Alternatives rejected.**

| Option | Why not |
|---|---|
| Azure Blob (flat, no HNS) | Directory operations become expensive object-by-object copies; Delta metadata operations degrade badly |
| Azure SQL Database | Relational storage is the wrong shape for raw semi-structured landing data, and cost scales with volume rather than usage |
| Azure Synapse dedicated pool | Costs an order of magnitude more; the project needs a lake, not an MPP warehouse |

**Trade-offs.** Object storage has higher per-operation latency than a database.
Small files hurt disproportionately, which is why `OPTIMIZE` matters. There is
no native referential integrity — enforcing it is the pipeline's job, which is
exactly what Silver's referential rules do.

---

## D2 — Azure Data Factory for ingestion and orchestration

**Context.** Something has to move data from source into the lake on a schedule,
manage the watermark, invoke Databricks, and fail visibly.

**Chosen.** ADF, restricted to orchestration and data movement.

**Why.** ADF's Copy activity handles source connectivity, retry and parallelism
declaratively. Its Lookup + Copy + Stored-procedure pattern for watermarks is
the canonical Azure incremental-load design, so it is immediately recognisable.
Crucially, it draws a clean boundary: **ADF moves, Databricks transforms.**

**Alternatives rejected.**

| Option | Why not |
|---|---|
| Databricks Jobs alone | Would work, but collapses ingestion and transformation into one tool. The resume claims ADF; more importantly, source connectivity in ADF is declarative rather than hand-written |
| Airflow | Another service to host and pay for. Out of scope (§5) |
| Azure Functions | Serverless compute is a poor fit for bulk data movement |
| Doing transformations in ADF Data Flows | Logic becomes untestable GUI configuration that cannot be unit-tested or run locally |

**Trade-offs.** ADF is GUI/JSON-centric, so it is harder to code-review and
diff than Python. Debugging a failed activity means reading the monitor UI. The
mitigation is keeping *nothing but orchestration* in it.

---

## D3 — Databricks + PySpark for transformation

**Chosen.** Azure Databricks, single-node, smallest viable SKU, with
auto-termination.

**Why.** Delta Lake is native, the runtime is tuned, and PySpark logic is plain
Python — testable, reviewable, and runnable locally against the same code path.
`RETAILINTEL_ENV` switches between local and Databricks with no code change,
which is what let the entire pipeline be developed and verified without spending
anything.

**Alternatives rejected.**

| Option | Why not |
|---|---|
| Pandas | Single-machine, no ACID, no MERGE, no schema enforcement. Fine at 130K rows, dead at 130M — and the point is an architecture that scales |
| Azure Synapse Spark | Comparable capability, weaker Delta tooling, out of the allowed service list |
| SQL stored procedures | Would require a database as the compute layer, defeating the lakehouse design |

**Trade-offs.** Spark has real fixed overhead — visible in the benchmark, where
sub-second queries are dominated by job setup rather than data processing. At
this volume Pandas would genuinely be faster. The architecture is chosen for
where it goes, not where it starts, and [METRICS.md](METRICS.md) says so
plainly.

---

## D4 — Delta Lake rather than plain Parquet

**Chosen.** Delta Lake for Silver and Gold. Bronze is also Delta for consistency.

**Why.** Parquet is a file format; Delta is a table format. Concretely, Delta
provides four things this project *actually uses*:

1. **ACID transactions** via the `_delta_log`. A failed job leaves no partial
   table. With bare Parquet, a crash mid-write leaves half-written files that
   the next reader happily reads.
2. **MERGE.** Upserting late corrections in Parquet means reading a partition,
   recomputing, and rewriting it — by hand, non-atomically.
3. **Schema enforcement.** A type mismatch is rejected at write time rather than
   discovered months later in a report.
4. **OPTIMIZE / ZORDER.** Compacts the small files every incremental MERGE
   creates.

Time travel is available and used for debugging, but is not load-bearing here.

**Alternatives rejected.** Plain Parquet (no ACID, no MERGE, manual compaction);
Apache Iceberg or Hudi (equivalent capability, but Delta is native to Databricks
and the ecosystem being demonstrated).

**Trade-offs.** Delta adds metadata overhead and a `_delta_log` that itself
needs maintenance (`VACUUM`). Reading it outside Spark is awkward — which is
precisely why the Power BI step exports CSV (see D9).

---

## D5 — Medallion (Bronze/Silver/Gold) rather than one transformation

**Chosen.** Three layers with distinct responsibilities.

**Why.** Each boundary buys something specific:

- **Bronze** makes reprocessing possible without going back to a source system
  that may have overwritten the row.
- **Silver** means cleaning happens *once*, consistently, with one definition of
  revenue — instead of every consumer inventing their own.
- **Gold** shapes data for questions rather than for storage.

The alternative — one script from CSV to report — means a rule change requires
re-extracting from source, and every consumer re-implements cleaning slightly
differently. That divergence is the single most common cause of "the two reports
disagree".

**Trade-offs.** Three copies of the data (~3× storage) and higher latency than a
direct path. At this volume storage is trivial; at scale, storage remains far
cheaper than re-extraction and re-litigation of business logic.

---

## D6 — Star schema rather than one wide denormalized table

**Chosen.** Four dimensions + two facts + one mart.

**Why.** A wide table repeats every product and store attribute on all 130K rows,
so a category rename becomes a full rewrite. The star schema keeps attributes in
one place, keeps dimensions small enough to broadcast (which is why star joins
are fast), and maps directly onto how Power BI expects to model data — one
direction, dimension to fact.

**Alternatives rejected.**

| Option | Why not |
|---|---|
| One denormalized wide table | Attribute updates rewrite the whole table; storage bloat; ambiguous grain |
| Snowflake schema | Normalising `category` out of `dim_product` adds a join to save a few KB. Not worth it at this dimension size |
| Data Vault | Enormous modelling overhead for a 5-table mini-project |

**Trade-offs.** Joins are required. Surrogate-key management is extra machinery.
Both are handled once, in `_assign_surrogate_keys()`.

---

## D7 — Incremental loading with a timestamp watermark

**Chosen.** `last_modified_timestamp` watermark, tracked per layer per table in
a Delta control table, advanced only after a successful write.

**Why.** It is the simplest mechanism that correctly handles the case that
actually matters: a transaction corrected days after the sale, arriving in a
later batch with its original transaction date. Partition-based loading ("load
yesterday") misses those entirely.

**Alternatives rejected.**

| Option | Why not |
|---|---|
| Full reload every run | Correct but wasteful, and gets linearly worse. Retained as `--full-reload` for rebuilds |
| Load by transaction date partition | **Silently wrong** — misses late corrections, which is the whole problem |
| True CDC (log-based) | Would capture deletes and be more precise, but needs source-system support that does not exist here. Claiming CDC without implementing it would be dishonest |

**Trade-offs — stated honestly.** This approach **cannot detect deletes**: a row
removed at source stays in the lakehouse forever. It depends on the source
maintaining `last_modified` correctly. And it uses a strict `>` comparison, so a
row written at exactly the watermark instant with a concurrent writer could be
missed — at which point you switch to `>=` and lean on the MERGE for
idempotency, which is safe because every write is keyed.

---

## D8 — Quarantine invalid rows rather than dropping or failing

**Chosen.** Rows failing a REJECT rule are written to a quarantine table with
the failed rule ids and the original raw values attached. The run continues.

**Why.** The two obvious alternatives are both wrong:

- **Dropping silently** loses the evidence. Revenue is understated and nobody
  knows why.
- **Failing the pipeline** on any bad row means one malformed record blocks
  135,000 good ones. Bad data is normal; a broken *contract* is not.

That distinction is why a *missing column* raises `SchemaValidationError` and
stops the run, while a *negative quantity* quarantines one row. One is a
pipeline failure, the other is a data-quality statistic.

**Trade-offs.** Quarantine needs monitoring or it silently accumulates — a
growing rejection rate is a signal that something upstream changed. There is no
automated re-processing path for corrected rows; that would be the next
increment.

---

## D9 — CSV export for Power BI rather than a live connection

**Chosen.** Export the Gold snapshot to CSV; Power BI Desktop imports it.

**Why.** Power BI Desktop cannot read a Delta table from a local folder — the
Delta connector targets Databricks and Fabric. Pointing Power BI at the raw
Parquet underneath would read *every version* in the transaction log rather than
the current snapshot, silently inflating every total.

**Alternatives rejected.** Databricks SQL warehouse with the native connector —
correct for production, and what a real deployment would do, but it bills for as
long as the report is open. Rejected purely on cost (§28).

**Trade-offs.** The export is a point-in-time snapshot that must be refreshed
after a pipeline run, and it loses Delta's schema metadata. Acceptable for a
report built once for screenshots.

---

## D10 — SCD Type 1 dimensions

**Chosen.** Overwrite dimension attributes in place, with a change-detection
hash so unchanged rows are not rewritten.

**Why.** The attributes here — category, brand, city, store format — are
corrections rather than history anyone needs to reconstruct. Type 2 would add
surrogate versioning, effective dates, current flags, and a materially more
complex fact-load join, to answer a question this project does not ask.

**Trade-offs.** Historical attribution is lost: if a product moves category, all
its history reports under the new category. For a *real* promotion analysis
that could matter, and Type 2 would be the correct answer. Named as a known
limitation rather than an oversight.

---

## D11 — Promotion ROI baseline: pre-period average

**Chosen.** Average daily revenue for the promoted product over the 28 days
immediately before the campaign, scaled to the campaign's duration.

**Why.** It is explainable in one sentence, computable from the data available,
and directional enough to rank campaigns against each other.

**Alternatives rejected.** Control-store holdout (the correct method — needs
stores deliberately excluded from the promotion, which synthetic data cannot
supply); year-on-year comparison (needs a prior year); regression-based demand
modelling (out of scope for a mini-project).

**Trade-offs — the ones that matter.** This baseline does **not** separate
genuine incremental demand from **pull-forward**, where a customer who would
have bought next week buys now. It also attributes the entire period change to
the campaign, so seasonality and payday effects are counted as uplift. Both
inflate measured ROI. This is why the margin-based variant is computed
alongside — and it tells a very different story (see [METRICS.md](METRICS.md)).

---

## D12 — Compute revenue once, in Silver

**Chosen.** `net_revenue = quantity × unit_price − discount_amount` is computed
in Silver and carried forward unchanged into Gold and Power BI.

**Why.** One definition, in one place. The alternative — each layer computing it
— guarantees that one day they disagree, and reconciling a Power BI measure
against a Spark aggregate is a miserable way to spend an afternoon. The
Silver→Gold reconciliation assertion in `consistency_report.py` exists to prove
they still agree.

**Trade-offs.** Slightly more storage on the fact; a redefinition requires
reprocessing rather than editing a DAX measure.

---

## D14 — Databricks reaches ADLS via a service principal, not a managed identity

**Context.** Everything else in this project authenticates with managed
identities — ADF to storage, ADF to Databricks. Databricks to storage is the one
exception, and it is worth explaining rather than hiding.

**Chosen.** A service principal (`sp-retailintel-databricks`) whose credentials
live in the Databricks secret scope `retailintel`, referenced from the cluster's
Spark config as `{{secrets/retailintel/...}}`.

**Why not a managed identity.** Managed-identity access to ADLS from Databricks
goes through **Unity Catalog external locations** backed by an Access Connector.
That requires a UC metastore, which is provisioned at the *account* level
(`accounts.azuredatabricks.net`) rather than through the Azure CLI, and a new
trial workspace does not necessarily have one attached. The SP + secret-scope
pattern is the standard pre-UC approach, works on any workspace tier, and is
fully scriptable.

**Trade-offs — stated plainly.** This is genuinely less clean than a managed
identity. A client secret exists, and it has an expiry that someone must rotate.
The mitigations are real but partial:

- The secret was written **directly into the Databricks secret scope** and never
  displayed, logged, or written to the repository.
- The SP is granted `Storage Blob Data Contributor` **scoped to the single
  storage account**, not the subscription.
- The cluster config contains only `{{secrets/...}}` references, which Databricks
  resolves at runtime — the committed JSON is safe.

In production with Unity Catalog available, the Access Connector + managed
identity route is the correct answer, and I would use it.

---

## D15 — Node type chosen by fallback list rather than hardcoded

**Context.** The first Databricks run failed with
`CLOUD_PROVIDER_RESOURCE_STOCKOUT`: `Standard_DS3_v2` was unavailable in
CentralIndia for this subscription.

**Diagnosis.** Not a quota problem — every VM family on the subscription allows
4 vCPUs, and usage was 0. It was pure regional **capacity**. Quota and capacity
are different failure modes and are easy to confuse; the error message names the
SKU, not the quota, which is the tell.

**Chosen.** `scripts/dbx_run.py` tries a list of six 4-core node types
(`Standard_D4ads_v5` first) and moves to the next when a stockout is reported.
The list was built by intersecting Databricks' supported node types with
`az vm list-skus` results that carry no location restriction.

**Why not just switch region.** Would also have worked, but it makes the
deployment fragile in a different way — the next region can stock out too. A
fallback list survives both.

**Trade-offs.** A failed cluster start costs ~4 minutes before the fallback
triggers, so a bad first choice is slow rather than fatal. Ordering the list by
observed availability keeps that cost rare.

---

## D16 — Delta maintenance via the Python API, not `OPTIMIZE delta.\`path\`` SQL

**Context.** The Gold build originally ran maintenance as SQL:

```sql
OPTIMIZE delta.`abfss://.../gold/fact_sales` ZORDER BY (store_key, product_key)
```

That works locally and failed on Azure Databricks with:

```
[UC_HIVE_METASTORE_DISABLED_EXCEPTION] The operation attempted to use Hive
Metastore for table `spark_catalog`.`delta`.`abfss://...`
```

**Diagnosis.** The `delta.\`<path>\`` syntax is resolved as a three-part name
through `spark_catalog`, which routes to the Hive metastore. New Unity
Catalog-enabled workspaces have legacy metastore access disabled, so the lookup
is rejected — even though the operation only ever needed a filesystem path.

**Chosen.**

```python
DeltaTable.forPath(spark, path).optimize().executeZOrderBy("store_key", "product_key")
```

The Python API addresses the path directly and never touches a catalog.

**Why this is the better default anyway.** Path-based SQL quietly depends on
catalog configuration that differs between a local session, a UC workspace and a
legacy workspace. The Python API behaves identically on all three, which is the
whole point of having one codebase run in both places.

**Trade-offs.** Slightly less readable than the SQL form, and it needs
`delta.tables` imported where previously a plain `spark.sql` sufficed. Worth it
to remove an environment-dependent failure that only appears in the cloud.

---

## D13 — Partition `fact_sales` by month, not by day

**Chosen.** `year_month` — four partitions of roughly 33K rows.

**Why.** Daily partitioning would create 120 partitions averaging a few hundred
rows. Per-file open cost would then exceed everything partition pruning saves —
the classic small-file problem.

**Trade-offs.** A single-day query scans a whole month. At this volume that is
milliseconds. At 100M rows the correct answer flips to daily partitions or
liquid clustering.
