# STUDY GUIDE

Teaches RetailIntel from zero. Every concept follows the same shape:

> **WHAT** it is · **WHY** it exists · **HOW** it works · **IN RETAILINTEL**
> where it actually lives · **EXAMPLE** · **INTERVIEW** likely question and a
> good answer

A suggested reading order is at the very bottom. If you have limited time, jump
to [§10 Interview priorities](#10--interview-priorities).

---

# 1 · Data engineering foundations

## 1.1 ETL and ELT

**WHAT.** Two orderings of the same three steps. **ETL** = Extract, Transform,
Load — transform *before* landing in the target. **ELT** = Extract, Load,
Transform — land the raw data first, transform *inside* the target system.

**WHY.** ETL comes from an era when storage was expensive and the target was a
rigid warehouse, so you cleaned data before paying to store it. Storage is now
cheap and compute is elastic, so ELT wins: land everything, then transform. If
your transformation logic turns out to be wrong, ELT lets you fix it and
reprocess from raw. ETL means going back to the source system — which may have
already overwritten the row.

**HOW.** ELT lands raw data in a lake, then uses the lake's own compute (Spark,
SQL) to transform it in place across layers.

**IN RETAILINTEL.** This is **ELT**. ADF extracts and loads to Bronze with *zero*
transformation (`src/bronze/ingest.py`), then Databricks transforms Bronze →
Silver → Gold. This is precisely why Bronze stores everything as STRING.

**EXAMPLE.** A source sends `quantity = "N/A"`.
- *ETL*: cast during ingestion → becomes `NULL` → the original value is gone
  forever and nobody can explain the rejection.
- *ELT*: land `"N/A"` as text → Silver reports "rule SAL_005 failed, raw value
  was `N/A`" → quarantined **with evidence**.

**INTERVIEW — "Is your project ETL or ELT, and why?"**
> ELT. ADF loads raw data into Bronze untransformed, and PySpark transforms
> inside the lakehouse. I chose it so I could reprocess from Bronze when a rule
> changes, without re-extracting from a source that may have moved on. It also
> preserves evidence — Bronze keeps the raw string, so a rejected row can be
> explained rather than just counted.

## 1.2 Batch vs streaming

**WHAT.** Batch processes a bounded set of data on a schedule. Streaming
processes unbounded events continuously as they arrive.

**WHY.** Batch is simpler, cheaper, and easier to reason about — you can rerun
it. Streaming is for when latency genuinely matters (fraud detection, live
inventory). Most analytics does not need it.

**IN RETAILINTEL.** Batch, deliberately. Promotion analysis is a weekly/monthly
decision; sub-second latency would add cost and complexity for no benefit.

**INTERVIEW — "Why not streaming?"**
> The business question is "did this campaign pay back", which is answered
> weekly at best. Streaming would add Event Hubs, checkpointing and
> exactly-once semantics to deliver freshness nobody asked for. I'd move to
> streaming if the use case were live stock-outs.

## 1.3 Data pipeline

**WHAT.** An automated sequence of steps moving data from source to a usable
form, with defined ordering, failure behaviour, and observability.

**WHY.** Without one, "the report" is a person and a spreadsheet — unrepeatable,
undocumented, and unavailable when that person is on leave.

**IN RETAILINTEL.** `src/pipeline.py` locally; ADF's
`pl_retailintel_medallion` in Azure. Ordering is enforced (masters before
transactions; Bronze before Silver before Gold), and each step logs to
`_control/pipeline_runs`.

## 1.4 OLTP vs OLAP

| | OLTP | OLAP |
|---|---|---|
| Purpose | Run the business | Analyse the business |
| Typical query | "Insert this sale" | "Revenue by region by month" |
| Rows touched | One or a few | Millions |
| Model | Normalised (3NF) | Dimensional (star) |
| Optimised for | Writes, row lookup | Reads, aggregation |
| Storage | Row-oriented | Column-oriented |

**WHY it matters.** They pull in opposite directions. A normalised OLTP schema
makes writes cheap and analytical joins expensive. A star schema makes
aggregation cheap and single-row updates awkward. Running heavy analytics
against a production OLTP database slows down the business.

**IN RETAILINTEL.** The simulated POS is OLTP-shaped (`transaction_id`,
normalised codes). Gold is OLAP-shaped (star schema, pre-computed
`net_revenue`, column-oriented Parquet under Delta).

**INTERVIEW — "Why not just query the source database?"**
> Three reasons: it competes with the business for resources; the normalised
> model needs many joins for every analytical question; and there's no history —
> the source holds the current state, so a corrected transaction overwrites its
> own past. The lakehouse keeps Bronze as an immutable record.

## 1.5 Data warehouse vs data lake vs lakehouse

**Data warehouse** — structured, schema-on-write, SQL, expensive, governed.
Great at BI; poor at semi-structured data and cost at scale.

**Data lake** — any file, any format, cheap object storage, schema-on-read.
Great at scale and flexibility; no ACID, no schema enforcement. Without
discipline it becomes a "data swamp" where nobody trusts anything.

**Lakehouse** — cheap object storage **plus** a table format (Delta) that adds
ACID transactions, schema enforcement, and upserts. Warehouse reliability at
lake cost.

**IN RETAILINTEL.** A lakehouse: ADLS Gen2 (lake economics) + Delta Lake
(warehouse guarantees) + a star schema in Gold (warehouse modelling).

**INTERVIEW — "What makes a lakehouse different from a lake with extra steps?"**
> The transaction log. Delta's `_delta_log` gives ACID, so a failed write leaves
> no partial table and a reader never sees half-written data. That's the thing a
> plain lake can't do, and it's why you can safely MERGE upserts into it.

---

# 2 · Azure

## 2.1 Resource Group

**WHAT.** A logical container for related Azure resources.

**WHY.** Lifecycle and access management. Everything in a group can share RBAC,
tags and — critically — be **deleted together**.

**IN RETAILINTEL.** `rg-retailintel` holds all four resources, so teardown is
one command. That's a deliberate cost-control decision (see
`AZURE_CLEANUP.md`).

**INTERVIEW — "Why one resource group?"**
> Shared lifecycle. Everything is created and destroyed together, so cleanup is
> a single operation and I can't orphan a billing resource. In production you'd
> split by lifecycle — a long-lived storage RG separate from ephemeral compute.

## 2.2 Storage Account and ADLS Gen2

**WHAT.** A Storage Account is Azure's object storage. **ADLS Gen2** is a
Storage Account with **Hierarchical Namespace (HNS)** enabled.

**WHY HNS is the whole point.** Flat blob storage has no real directories —
"folders" are just prefixes in a key. So renaming or deleting a "directory"
means copying or deleting every object individually: **O(n)** and slow. With
HNS, directories are real, and rename/delete are **atomic metadata operations**.

Delta Lake depends on cheap directory listing and atomic file operations. On
flat blob storage, Delta metadata operations degrade badly.

**IN RETAILINTEL.** `--enable-hierarchical-namespace true`. Two containers:
`landing` (source drop) and `lakehouse` (bronze/silver/gold/quarantine/_control).

**INTERVIEW — "ADLS Gen2 vs Blob Storage?"**
> Same underlying service; Gen2 adds the hierarchical namespace. That makes
> directory operations atomic instead of per-object copies, gives POSIX-style
> ACLs, and is what Delta needs to perform well. For analytics it's Gen2 every
> time; plain Blob is fine for storing unrelated files.

## 2.3 Authentication vs authorization, and RBAC

**Authentication** = *who are you* (proving identity).
**Authorization** = *what may you do* (permissions).

**RBAC** = Role-Based Access Control: assign a **role** (a set of permissions) to
a **principal** (user, group, managed identity) at a **scope** (subscription,
resource group, resource).

**Managed identity** — an Azure-managed service principal attached to a resource.
The key benefit: **no credential exists to leak**. Azure handles issuing and
rotating tokens.

**IN RETAILINTEL.** ADF and Databricks authenticate to storage using **managed
identities** granted `Storage Blob Data Contributor` **scoped to the one storage
account**. No account key, no connection string, nothing in code or git.

**INTERVIEW — "How does Databricks access storage without credentials?"**
> A managed identity with a role assignment. The alternative — a storage account
> key — can't be scoped to a workload, can't be revoked without breaking
> everything else that uses it, and tends to end up in a notebook. A role
> assignment is scoped, auditable and revocable independently.

**INTERVIEW — "Difference between the `Contributor` and `Storage Blob Data Contributor` roles?"**
> `Contributor` is a *control-plane* role — it can create and delete the storage
> account but cannot read the data inside it. `Storage Blob Data Contributor` is
> a *data-plane* role — it can read and write blobs. People are constantly
> caught out by having Contributor and still getting 403 on data access.

---

# 3 · Azure Data Factory

## 3.1 The building blocks

| Concept | What it is | In RetailIntel |
|---|---|---|
| **Linked Service** | A connection definition — the "where and how to connect" | `ls_adls_gen2`, `ls_databricks` |
| **Dataset** | A named view of data *through* a linked service — the "what" | `ds_landing_csv`, `ds_raw_csv` |
| **Activity** | A single unit of work | Copy, ForEach, DatabricksNotebook |
| **Pipeline** | An ordered set of activities with dependencies | `pl_retailintel_medallion` |
| **Trigger** | What starts a pipeline (schedule, tumbling window, event) | **None deployed — on purpose** |
| **Integration Runtime** | The compute that executes activities | Azure-hosted (default) |

The relationship: a **Dataset** points at a **Linked Service**; an **Activity**
uses Datasets; a **Pipeline** orders Activities; a **Trigger** starts Pipelines.

## 3.2 Copy Activity

**WHAT.** ADF's data-movement workhorse: source → sink, with format conversion,
retry and parallelism handled declaratively.

**IN RETAILINTEL.** Copies each landing CSV into the lakehouse `raw/` zone.
It performs **no transformation** — that boundary is the point.

**INTERVIEW — "Why not transform in ADF Data Flows?"**
> Because it becomes untestable GUI configuration. My transformation logic is
> PySpark, so it has 59 unit tests and runs identically on my laptop and on
> Databricks. Mapping Data Flows can't be unit-tested or code-reviewed as a
> diff, and they silently spin up Spark clusters anyway — so you pay Spark
> prices for less control.

## 3.3 Parameters vs variables

- **Parameters** are inputs, set at invocation, **immutable** during the run.
- **Variables** are mutable state **inside** a run (`SetVariable`, `AppendVariable`).

**IN RETAILINTEL.** `batch_id` and `full_reload` are pipeline *parameters*.
`source_list` is a *variable*. Datasets take `batch_id` and `source_name` as
parameters, so **one dataset definition serves all five feeds across all
batches** instead of 15 near-identical definitions.

**INTERVIEW — "How did you avoid duplicating pipelines per table?"**
> Parameterised datasets plus a ForEach. One Copy activity definition iterates a
> source list, and the dataset's folder path is built from the parameters. Adding
> a sixth feed is one array entry, not a new pipeline.

## 3.4 Incremental loading and watermarks

**WHAT.** A **watermark** is a stored high-water mark of what has already been
successfully processed — here, the maximum `last_modified_timestamp`.

**WHY.** Reloading everything works until it doesn't: cost and runtime grow
linearly forever.

**HOW — the loop.**
```
read watermark
  → SELECT rows WHERE last_modified_timestamp > watermark
  → transform
  → write (MERGE)
  → ON SUCCESS ONLY: watermark = MAX(last_modified_timestamp) seen
```

**The ordering is load-bearing.** The watermark advances *last*. If the write
fails, the watermark stays put and the next run re-reads the same window.
Re-reading is safe because every write is a MERGE on a business key, so replay
converges rather than duplicating.

**IN RETAILINTEL.** Each layer keeps its **own** watermark in
`_control/watermarks`, so Silver can be rebuilt from Bronze without
re-extracting from source. Measured across three batches: **1,752 rows correctly
filtered** as already-processed, **405 late corrections** applied by MERGE.

**Why the watermark is on `last_modified`, not transaction date.** ~1.5% of
transactions are corrected days after the sale and arrive in a later batch
carrying their *original* transaction date. A "load yesterday's partition" job
misses them entirely.

**INTERVIEW — "Why not use ADF's Lookup + stored-procedure watermark pattern?"**
> That's the canonical pattern and it assumes a queryable source. Mine is CSV
> files, and Copy can't filter on file contents. But there's a better reason:
> keeping the watermark in the Delta control table means it advances in the same
> logical unit of work as the data write. If ADF's MERGE succeeds and its
> "update watermark" activity then fails, ADF's watermark is wrong and the next
> run silently skips or reprocesses. Storing it next to the data removes that
> failure mode.

**INTERVIEW — "Is this CDC?"**
> **No, and I'm careful not to claim it.** It's timestamp-based incremental
> extraction. Real CDC reads the database log and captures deletes. Mine can't
> detect a delete — a row removed at source stays in the lakehouse — and it
> depends on the source maintaining `last_modified` honestly.

## 3.5 Triggers and monitoring

**Trigger types:** Schedule (wall clock), Tumbling Window (fixed
non-overlapping slices, supports backfill and dependencies), Event
(blob created/deleted), Manual.

**IN RETAILINTEL.** **No triggers deployed.** Runs are manual, because a
schedule would burn activity runs for a project that only needs demonstrating.
`AZURE_CLEANUP.md` calls out disabling triggers first — a forgotten trigger is
the classic way to wake up to a bill.

**Monitoring** is the Monitor tab: run history, per-activity duration, rows
read/written, error details, and rerun-from-failed-activity.

---

# 4 · Databricks and Spark

## 4.1 Workspace, cluster, notebook

- **Workspace** — the Databricks environment: notebooks, jobs, clusters, repos.
- **Cluster** — the Spark compute. **All-purpose** (interactive, stays up, you
  pay while it lives) vs **Job** (created for a run, **terminates itself**).
- **Notebook** — cells of code, useful for exploration and as a job entry point.

**IN RETAILINTEL.** ADF's linked service uses a **single-node job cluster**, so
compute is created per run and dies with it. The notebooks in `notebooks/` are
deliberately thin wrappers — all logic lives in `src/` so it can be unit-tested.

**INTERVIEW — "Why is your logic in `src/` rather than in notebooks?"**
> Notebooks can't be unit-tested or reviewed properly as a diff, and cell
> execution order makes them non-deterministic. My notebooks set widgets, import
> from `src/`, call one function, and display results. That's why I have 59
> tests, and why the same code runs on my laptop and on Databricks — only the
> storage root changes, via `RETAILINTEL_ENV`.

## 4.2 Driver and executors

- **Driver** — runs your program, builds the execution plan, schedules tasks,
  collects results. One per application.
- **Executor** — worker process; runs tasks on partitions, holds cached data.
- **Task** — the unit of work: one operation on one partition.

**Danger.** `.collect()` pulls *all* data to the driver. On a big DataFrame this
OOMs the driver.

**IN RETAILINTEL.** Single-node: driver and executor on one machine. Fine at
130K rows. `.collect()` is only ever called on aggregates (a single row) or on
tiny dimension lookups.

**INTERVIEW — "Your job runs out of memory on the driver. Why?"**
> Almost always `collect()`, `toPandas()`, or a broadcast of something too big.
> The fix is to keep work distributed — write to a table instead of collecting,
> or aggregate first so what comes back is small.

## 4.3 Transformations, actions, lazy evaluation

**Transformations** (`select`, `filter`, `join`, `groupBy`) are **lazy** — they
build a plan, they don't execute.
**Actions** (`count`, `collect`, `write`, `show`) **trigger** execution.

**WHY laziness matters.** Because Spark sees the whole plan before running it,
Catalyst can optimise across steps — pushing filters down to the scan, pruning
unread columns, reordering joins. Eager execution would forfeit all of that.

**EXAMPLE.**
```python
df = spark.read.parquet(path)        # nothing happens
df2 = df.filter(col("x") > 10)       # nothing happens
df3 = df2.select("a", "b")           # nothing happens
df3.count()                          # NOW it runs — and reads only a, b,
                                     # with x > 10 pushed into the file scan
```

**The trap.** Every action **re-executes the whole lineage**. Calling `count()`
then `write()` computes it twice — which is exactly what `.cache()` is for.

**IN RETAILINTEL.** `src/silver/build_silver.py` caches `prepared` because it is
counted, aggregated per-rule, split into valid/invalid, and written — four
actions over the same expensive computation.

**INTERVIEW — "What is lazy evaluation and why does it help?"**
> Transformations only build a plan; actions execute it. Because Spark sees the
> whole plan at once it can push filters into the scan and prune columns, so it
> reads far less data. The catch is that each action re-runs the lineage, so
> anything reused needs caching.

## 4.4 Partitions and shuffle

**Partition** — a chunk of data processed by one task. Parallelism = partitions.

**Shuffle** — redistributing data across partitions so related rows meet. Caused
by `groupBy`, `join`, `distinct`, `orderBy`. It writes to disk and moves data
over the network: **the most expensive thing in Spark.**

**`spark.sql.shuffle.partitions`** defaults to **200**. On small data that means
200 nearly-empty partitions — 200 tasks of scheduling overhead to process almost
nothing.

**IN RETAILINTEL.** Set to **8** (`config/project_config.py`). At 130K rows, 200
partitions would cost far more in task overhead than the parallelism gains.

**INTERVIEW — "Your job creates thousands of tiny files. Why, and how do you fix it?"**
> Output file count follows partition count, so a 200-partition shuffle writes
> 200 files. Fix it by tuning `shuffle.partitions` to the data size, using
> `coalesce`/`repartition` before writing, and running Delta `OPTIMIZE` to
> compact. Small files hurt because every read pays per-file open cost.

## 4.5 Joins and broadcast

**Shuffle hash / sort-merge join** — both sides shuffled so matching keys land
together. Expensive but works at any size.

**Broadcast join** — the small side is copied to every executor, so the large
side never moves. **Dramatically** faster when one side is small.

**IN RETAILINTEL.** Every dimension is broadcast (`F.broadcast(...)` throughout
`build_gold.py`). Dimensions are KB-sized — **this is exactly why star schemas
are fast**: the fact table never shuffles.

**INTERVIEW — "Why are star-schema joins fast in Spark?"**
> Because dimensions stay small enough to broadcast. The big fact table stays
> put and each executor gets its own copy of the dimension, so the join is local
> and there's no shuffle. That property is a direct consequence of the modelling
> choice.

## 4.6 Aggregations

`groupBy().agg()` triggers a shuffle, but Spark does a **partial aggregation**
first on each partition, then combines. So `sum` moves very little data;
`collect_list` moves everything.

**Watch out:** `countDistinct` is expensive (needs global visibility).
`approx_count_distinct` uses HyperLogLog when an estimate is acceptable.

---

# 5 · Delta Lake

## 5.1 Parquet vs Delta

**Parquet** is a **file format**: columnar, compressed, with per-column
statistics. Columnar means a query touching 2 of 20 columns reads only those 2.

**Delta** is a **table format**: Parquet data files **plus** a transaction log
(`_delta_log`) of JSON commits.

**A file format cannot give you:** atomicity, isolation, upserts, schema
enforcement, or time travel. A table format can.

**INTERVIEW — "Delta vs Parquet — what do you actually gain?"**
> Delta *is* Parquet plus a transaction log. The log gives ACID, so a failed job
> leaves no partial table; MERGE, so I can upsert late corrections; schema
> enforcement, so a type mismatch is rejected at write time rather than
> discovered in a report; and OPTIMIZE to compact the small files every
> incremental MERGE creates. I use all four.

## 5.2 The transaction log

**HOW.** Every write creates a numbered JSON commit in `_delta_log/`
(`000.json`, `001.json`, …) listing files added and removed. A reader reads the
log to learn the current file set. Periodically, checkpoints in Parquet
summarise the log so readers don't replay every commit.

**Consequences that matter:**
- **Atomicity** — the commit is the last step. Until it lands, nobody sees
  anything. A crash mid-write leaves orphan data files that no reader sees.
- **Isolation** — readers read a consistent snapshot while a write is in
  progress.
- **Time travel** — old commits still reference old files, so past versions are
  readable until `VACUUM` removes them.
- **Never read the Parquet files directly.** You'd see every version at once.
  This is exactly why Power BI gets a CSV export rather than being pointed at
  the folder.

## 5.3 ACID

| | Meaning | In Delta |
|---|---|---|
| **A**tomic | All or nothing | Commit is one log entry |
| **C**onsistent | Constraints hold | Schema enforcement |
| **I**solated | Concurrent ops don't interfere | Snapshot isolation, optimistic concurrency |
| **D**urable | Committed = permanent | Log + files on ADLS |

## 5.4 MERGE and upsert

**WHAT.** "Update if the key exists, insert if it doesn't" — in one atomic
operation.

```sql
MERGE INTO target t USING source s ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.last_modified > t.last_modified THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Two subtleties that are pure interview gold:**

**(a) The `last_modified >` guard.** Without it, replaying an *old* batch
overwrites a newer correction with stale values and silently corrupts the table.
Tested in `tests/test_dedup_and_merge.py::test_replaying_an_old_batch_does_not_revert_a_newer_correction`.

**(b) You must deduplicate the source first.** MERGE **errors** if one target row
matches multiple source rows — it can't decide which wins. Since the source
deliberately re-sends transactions, a batch legitimately contains the same id
twice.

**INTERVIEW — "Why MERGE instead of append?"**
> About 1.5% of transactions are corrected after the sale and arrive in a later
> batch with their original transaction date. Appending would leave both the
> original and the correction in the table and overstate revenue. MERGE updates
> in place, so the table always holds exactly one current row per transaction —
> and running the same batch twice changes nothing.

## 5.5 Schema enforcement vs schema evolution

**Enforcement** — Delta rejects a write whose schema doesn't match. This is a
*feature*: it stops a source type change silently corrupting a column.

**Evolution** — `mergeSchema` explicitly permits adding new columns.

**IN RETAILINTEL.** Enforcement is the default everywhere. `mergeSchema` is
enabled **only** on quarantine tables, where rule sets legitimately change over
time. Note the deliberate asymmetry: a *new* column is tolerated by
`validate_structure()`, a *missing* one fails the run.

## 5.6 Time travel

```sql
SELECT * FROM delta.`/path` VERSION AS OF 5
SELECT * FROM delta.`/path` TIMESTAMP AS OF '2025-04-01'
```

Useful for debugging ("what did this table look like before the bad run?"),
auditing, and rollback via `RESTORE`. **`VACUUM` deletes files older than the
retention period (default 7 days) and ends time travel beyond it.**

## 5.7 OPTIMIZE and Z-ORDER

**The small-file problem.** Every incremental MERGE writes new small files. Left
alone, a table becomes thousands of tiny files and every scan pays per-file open
cost.

**OPTIMIZE** rewrites them into fewer, larger files.
**Z-ORDER** co-locates rows by column value, so filtered queries skip more files
(data skipping uses per-file min/max statistics).

**IN RETAILINTEL.** After every Gold build:
```sql
OPTIMIZE delta.`.../fact_sales` ZORDER BY (store_key, product_key, campaign_key)
```
Z-ORDER columns are chosen as the ones reports actually filter on.

---

# 6 · Medallion architecture

## 6.1 The three layers

| Layer | Purpose | Rule |
|---|---|---|
| **Bronze** | Raw, as received + lineage | Never transform |
| **Silver** | Typed, validated, deduplicated | Clean **once**, for everyone |
| **Gold** | Modelled for consumption | Shape for questions |

**WHY three, not one?** Each boundary buys something specific:
- **Bronze** makes reprocessing possible without the source system.
- **Silver** means cleaning happens once with **one definition of revenue**,
  instead of every consumer inventing their own. That divergence is the single
  most common cause of "the two reports disagree".
- **Gold** shapes data for questions rather than for storage.

**Cost:** ~3× storage and higher latency. Storage is cheap; re-litigating
business logic is not.

**INTERVIEW — "Isn't three copies wasteful?"**
> Storage is the cheapest thing in the stack. What it buys is the ability to
> reprocess from Bronze when a rule changes, and one authoritative definition of
> revenue in Silver. Without Silver, every consumer re-implements cleaning
> slightly differently and the numbers drift apart.

## 6.2 Data quality: two failure classes

The most important distinction in the whole project:

| | Structural failure | Row-level failure |
|---|---|---|
| Example | `store_id` column missing | `quantity = -3` |
| Meaning | The **contract** is broken | One **record** is unusable |
| Response | **Fail the run** | **Quarantine the row, continue** |

Getting this backwards gives you either a pipeline that dies on one bad row, or
one that silently swallows a renamed column and nulls a whole measure.

## 6.3 Quarantine

**WHAT.** Invalid rows written to a separate table **with the failed rule ids and
the original raw values attached**.

**WHY not just drop them?** Dropping loses the evidence — revenue is understated
and nobody knows why. Quarantine makes rejection **explainable and countable**.

**IN RETAILINTEL.** 2,002 rows quarantined (1.48%), each carrying
`_dq_failed_rules` like `SAL_006,SAL_010`. Partitioned by batch so re-running
replaces rather than appends.

## 6.4 Data lineage

**WHAT.** The ability to trace any row back to where it came from.

**IN RETAILINTEL.** Every Bronze row carries `_ingest_run_id`,
`_ingest_timestamp`, `_source_file`, `_batch_id`; Silver adds `_silver_run_id`.
Those join to `_control/pipeline_runs`, so any figure in Power BI can be traced
to the run and file that produced it.

---

# 7 · Dimensional modelling

## 7.1 Fact vs dimension

**Fact** — measurements. Numeric, additive, many rows. *"What happened."*
**Dimension** — descriptive context. Textual, few rows, used to slice.
*"Who, what, where, when."*

Rule of thumb: if you'd **sum** it, it's a fact measure; if you'd **group by**
it, it's a dimension attribute.

## 7.2 Grain — the most important decision

**WHAT.** What exactly one row of a fact table represents.

**WHY it comes first.** Grain determines what can be joined, what can be summed,
and whether double-counting is possible. Get it wrong and every number is
wrong.

**IN RETAILINTEL.**
- `fact_sales`: **one sales transaction** — one product, one store, one moment.
- `fact_inventory_snapshot`: **one product × one store × one weekly snapshot**.

**INTERVIEW — "What's the grain of your fact table?"** *(Expect this. It's the
single most common dimensional-modelling question.)*
> One row per sales transaction — one product sold at one store at one point in
> time. `transaction_id` is the primary key, a degenerate dimension. I state it
> explicitly because everything else follows from it: it's why MERGE keys on
> `transaction_id`, and why a duplicate at that grain would directly overstate
> revenue.

## 7.3 Additivity — and the trap

| Type | Can sum across | Example |
|---|---|---|
| **Additive** | All dimensions | `net_revenue`, `quantity` |
| **Semi-additive** | Some, **not time** | `stock_quantity` |
| **Non-additive** | None | `unit_price`, ratios, percentages |

**The trap.** `stock_quantity` is **semi-additive**. Summing 17 weekly snapshots
of the same 1,000 units claims **17,000 units that never existed**. You sum
*within* a date and **average** *across* dates — which is exactly what the
`Avg Inventory Value` DAX measure does.

**INTERVIEW — "What is a semi-additive measure?"**
> One you can sum across some dimensions but not time. Stock levels and account
> balances are the classic cases. In my model, summing weekly stock snapshots
> would count the same physical units once per week, so the DAX measure sums
> within a snapshot date and averages across dates.

## 7.4 Keys

- **Natural / business key** — the source's identifier (`product_id = "P0001"`).
- **Primary key** — uniquely identifies a row.
- **Foreign key** — points at another table's PK.
- **Surrogate key** — a meaningless integer the pipeline generates
  (`product_key = 42`).
- **Degenerate dimension** — an identifier kept **on the fact** with no
  dimension table of its own (`transaction_id`).

**WHY surrogate keys.** Integer joins beat repeated string comparisons; they
insulate the model from a source renumbering its keys; and they're required for
SCD Type 2, where one natural key legitimately has several rows.

**IN RETAILINTEL.** Keys are assigned once on first insert and **never
reissued** — a MERGE updates a dimension row's attributes but leaves its key
alone, so existing facts keep pointing at the right row.

## 7.5 Star schema

**WHAT.** One central fact table joined to dimensions — one join hop each.

**WHY.** Few joins, small broadcastable dimensions, and a shape BI tools expect.
Contrast a **snowflake** (dimensions normalised further — more joins to save
trivial space) and a **wide denormalized table** (no joins, but every attribute
repeated on 130K rows, so a category rename is a full rewrite).

**INTERVIEW — "Star vs snowflake vs one big table?"**
> Star. A snowflake would normalise `category` out of `dim_product` to save a
> few KB and add a join to every query. One big table avoids joins but repeats
> every attribute across 130K rows, so an attribute change rewrites everything
> and the grain becomes ambiguous. Star keeps dimensions small enough to
> broadcast, which is what makes the joins cheap.

## 7.6 Unknown members — the subtle one

**PROBLEM.** A sale references a campaign that isn't in the campaign master. If
`campaign_key` is NULL, an INNER join **silently drops the row** and revenue
quietly disappears.

**SOLUTION.** Every dimension has an **Unknown** member (key `-1`);
`dim_campaign` also has **No Campaign** (key `0`). Unresolvable references point
at Unknown, so facts join INNER and lose nothing.

**Why `0` and `-1` are kept apart:** *"not promoted"* and *"we don't know"* are
different answers. Collapsing them would inflate the baseline that promotion ROI
is measured against.

**IN RETAILINTEL.** 198 rows point at Unknown campaign — and are counted as
consistency defects in `METRICS.md` rather than hidden.

## 7.7 SCD Type 1 vs Type 2

**Type 1** — overwrite. No history. Simple.
**Type 2** — new row per change, with `valid_from`/`valid_to`/`is_current`.
Preserves history, so a fact joins to the attribute value **as it was at the
time**.

**IN RETAILINTEL.** **Type 1**, with a SHA-256 change-detection hash so
unchanged rows aren't rewritten.

**INTERVIEW — "Why Type 1? When would you need Type 2?"**
> The attributes here — category, brand, city — are corrections rather than
> history anyone needs to reconstruct, and Type 2 adds versioning, effective
> dates and a more complex fact-load join to answer a question this project
> doesn't ask. I'd need Type 2 if a product moved category and I had to report
> historical sales under the *old* category. Right now all history reports under
> the current one — I list that as a known limitation, not an oversight.

---

# 8 · Analytics and Power BI

## 8.1 KPI and the measures here

**Revenue** — `quantity × unit_price − discount_amount`. Defined **once**, in
Silver, and carried forward. One definition means Gold and Power BI can't
disagree — and a reconciliation assertion proves they still match (variance:
0.00).

**Gross profit** — `net_revenue − COGS`, where `COGS = quantity × unit_cost`.

**Inventory turnover** — `COGS ÷ average inventory value`, **both at cost**.
Mixing retail-valued stock with cost-valued COGS would understate turns.
Measured: **3.17 turns** over four months.

**Promotion ROI**
```
baseline_daily_revenue    = revenue for the product over the 28 days before ÷ 28
expected_baseline_revenue = baseline_daily_revenue × campaign duration
incremental_revenue       = actual campaign revenue − expected_baseline_revenue
roi                       = (incremental_revenue − campaign_cost) ÷ campaign_cost
```

**Baseline** — what would have happened *anyway*. Without it you'd credit the
campaign with all sales, not the extra ones.
**Incremental revenue** — the part attributable to the campaign.

**⭐ THE KEY FINDING — know this cold.** Revenue-based portfolio ROI is
**+0.42**. Margin-based ROI is **−1.00**. Incremental revenue was **+₹110,425**
but incremental **gross profit** was **−₹270** — the discount given away almost
exactly cancelled the margin on the extra volume.

**INTERVIEW — "Walk me through your promotion ROI."**
> I take the promoted product's revenue over the 28 days before the campaign,
> get a daily average, scale it to the campaign length — that's what would have
> happened anyway. Actual minus that is incremental revenue, and ROI is
> incremental minus cost, over cost. The interesting part is that I computed it
> both on revenue and on margin, and they disagree completely: +0.42 versus
> −1.00. The discount given away cancels the gross profit from the extra units.
> That's a very common retail effect, and it's why a revenue-only ROI is the
> number that gets a campaign renewed while the margin ROI is the number that
> should decide it.

**Limitations — say these before you're asked.** The baseline doesn't separate
genuine incremental demand from **pull-forward** (someone who'd have bought next
week buying now), and it attributes seasonality inside the window to the
campaign. A defensible read needs **control stores** — stores deliberately
excluded from the promotion.

## 8.2 Power BI concepts

- **Semantic model** — tables + relationships + measures. The reusable model
  layer under the visuals.
- **Relationship** — how tables join. **Cardinality** (one-to-many) and
  **direction** (single vs bidirectional).
- **Measure** — DAX evaluated **at query time** in filter context. Dynamic.
- **Calculated column** — evaluated at refresh, stored in the model. Static.

**Prefer measures.** Columns consume memory and can't respond to filter context.

**Avoid bidirectional relationships** — they create ambiguous filter paths and
subtle wrong numbers.

**IN RETAILINTEL.** All relationships are single-direction, dimension → fact.
`mart_campaign_roi` connects **only** to `dim_campaign`, deliberately: also
joining it to `dim_product` would create a loop
(`dim_product → fact_sales → dim_campaign → mart → dim_product`).

## 8.3 Filter context and basic DAX

**Filter context** — the set of filters (slicers, rows, columns) applying when a
measure evaluates. The core DAX concept.

```dax
Total Revenue = SUM ( fact_sales[net_revenue] )
```
On a row for "Mumbai", filter context restricts it to Mumbai automatically.

```dax
Promoted Revenue = CALCULATE ( [Total Revenue], fact_sales[is_promoted] = TRUE() )
```
`CALCULATE` **modifies** filter context — the single most important DAX function.

```dax
Portfolio ROI = DIVIDE ( [Incremental Revenue] - [Campaign Cost], [Campaign Cost] )
```
`DIVIDE` returns BLANK on divide-by-zero instead of an error.

**A real modelling decision in this project:** `Portfolio ROI` recomputes from
summed components rather than averaging the per-campaign `roi` column. Averaging
ratios weights a tiny campaign the same as a large one and produces a number
that doesn't reconcile to the totals.

**INTERVIEW — "Measure or calculated column?"**
> Measure by default. Measures evaluate at query time in filter context, so they
> respond to slicers and cost no memory. Calculated columns are computed at
> refresh and stored, so they bloat the model and can't react to user selection.
> I'd use a column only when I need to slice or group by the value itself.

---

# 9 · Reliability

## 9.1 Idempotency

**WHAT.** Running the same operation twice produces the same result as once.

**WHY.** Pipelines get rerun — retries, backfills, someone clicking twice. If a
rerun duplicates data, every number is wrong and the fix is manual.

**IN RETAILINTEL — three independent mechanisms:**
1. **Watermark** — an already-processed window returns zero rows.
2. **`replaceWhere` on `_batch_id`** — re-ingesting a batch replaces its
   partition rather than appending.
3. **MERGE on business key** — a re-presented row updates.

**Verified, not assumed:** `src/quality/idempotency_check.py` snapshots every
curated table, replays a processed batch, and diffs. Result: **zero rows
changed**.

**INTERVIEW — "What happens if your pipeline runs twice?"**
> Nothing changes, and I verified it rather than assuming. I replay an
> already-processed batch and diff every curated table before and after — zero
> difference in rows and revenue. Three things produce that: the watermark
> returns no rows, Bronze uses `replaceWhere` per batch, and Silver and Gold
> MERGE on business keys. The check would fail if any one were missing.

## 9.2 Failure handling

Never swallow errors, and never confuse the classes:

| Class | Example | Response |
|---|---|---|
| Data quality | Negative quantity | Quarantine, continue |
| Configuration | Missing column | **Fail loudly** |
| Infrastructure | Storage unreachable | Fail, retry; watermark unmoved |
| Transformation | Bug in code | Fail; watermark unmoved |

Because the watermark only advances on success, **any** failure leaves the
pipeline safely re-runnable.

---

# 10 · Interview priorities

If you only have a few hours, learn these in order. They're ranked by how likely
you are to be asked and how badly a weak answer lands.

### Tier 1 — you will be asked these
1. **Grain of your fact table** (§7.2)
2. **Why Delta over Parquet** (§5.1)
3. **How incremental loading works, and why not CDC** (§3.4)
4. **What happens if the pipeline runs twice** (§9.1)
5. **Why Bronze/Silver/Gold rather than one step** (§6.1)
6. **Star schema vs one wide table** (§7.5)

### Tier 2 — strong differentiators
7. **Why MERGE, and why dedup must come first** (§5.4)
8. **Structural vs row-level failure** (§6.2)
9. **Semi-additive measures** (§7.3)
10. **Unknown dimension members** (§7.6)
11. **The revenue vs margin ROI finding** (§8.1)
12. **Why ADF orchestrates but doesn't transform** (§3.2)

### Tier 3 — depth if it comes up
13. Lazy evaluation and caching (§4.3)
14. Shuffle and partition tuning (§4.4)
15. Broadcast joins and why star schemas are fast (§4.5)
16. SCD Type 1 vs 2 (§7.7)
17. Managed identity vs storage keys (§2.3)
18. DAX filter context (§8.3)

### The three things that make you sound senior

**1. Volunteer your limitations.** "The star-schema-only performance isolation
came out inside measurement noise, so I don't claim it — the 65–75% against the
manual workflow is the number that holds up." Nothing builds credibility faster
than a candidate who knows which of their own numbers are weak.

**2. Explain a trade-off, not just a choice.** Every entry in `DECISIONS.md` has
alternatives and costs. "I chose X" is junior. "I chose X over Y because Z, and
it costs me W" is not.

**3. Have one genuinely interesting finding.** Yours is the ROI split: +0.42 on
revenue, −1.00 on margin. It shows you interrogated your own output instead of
shipping the first number that looked good.

---

# 11 · Recommended study sequence

**Session 1 — orientation (1h)**
`README.md` → `ARCHITECTURE.md` → run the pipeline once and watch the output.

**Session 2 — the code (2h)**
`config/project_config.py` → `src/common/schemas.py` → `src/bronze/ingest.py` →
`src/silver/build_silver.py` → `src/gold/build_gold.py`. Read the module
docstrings; they explain the *why*.

**Session 3 — concepts (2h)**
This guide, §1–§5.

**Session 4 — modelling and analytics (2h)**
This guide, §6–§8, then `DATA_DICTIONARY.md` and `powerbi/measures.dax`.

**Session 5 — evidence (1h)**
`METRICS.md` and `DATA_QUALITY.md`. Know which claims hold and which don't.

**Session 6 — defence (1h)**
`DECISIONS.md` cover to cover, then §10 above. Say the Tier 1 answers **out
loud** — the gap between understanding something and explaining it fluently is
larger than it feels.
