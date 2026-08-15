"""Gold layer — dimensional (star schema) model.

    DimDate ─┐
    DimProduct ─┼─→ FactSales ←─ DimCampaign
    DimStore ─┘        │
                       └── FactInventorySnapshot (DimDate, DimProduct, DimStore)

Grain — stated explicitly, because everything else follows from it
------------------------------------------------------------------
FactSales:              one row per sales transaction — that is, one product
                        sold at one store at one point in time. transaction_id
                        is the primary key (a degenerate dimension: it is an
                        identifier we keep on the fact, not a dimension table).

FactInventorySnapshot:  one row per product, per store, per weekly snapshot
                        date. This is a *periodic snapshot* fact: its measure
                        (stock_quantity) is semi-additive — it may be summed
                        across products and stores, but never across dates.
                        Summing stock over time is meaningless; it would count
                        the same physical units once per week.

Surrogate keys
--------------
Every dimension has an integer surrogate key that the business never sees.
Integer joins beat repeated string comparisons, and a surrogate insulates the
model from a source system renumbering its natural keys. Keys are assigned once
on first insert and never reissued — a MERGE updates a dimension row's
attributes but leaves its key alone, so existing facts keep pointing at the
right row.

Special dimension members
-------------------------
Every dimension carries an Unknown member (key -1), and DimCampaign also carries
a "No Campaign" member (key 0). This is what lets FactSales be joined with an
INNER join and still lose nothing: a sale with an unresolvable campaign points
at Unknown rather than at NULL. NULL foreign keys would silently disappear from
inner-joined reports, which is exactly the kind of quiet undercount that makes
people stop trusting a dashboard.

Dimensions are SCD Type 1 (overwrite in place). The attributes here — category,
brand, city, format — are corrections rather than history worth preserving, and
Type 2 would add versioning machinery this project does not need. DECISIONS.md
records the trade-off.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.project_config import (  # noqa: E402
    BASELINE_WINDOW_DAYS,
    DIM_DATE_END,
    DIM_DATE_START,
    GOLD_TABLES,
    SILVER_TABLES,
)
from src.common.control import (  # noqa: E402
    get_watermark,
    log_run,
    set_watermark,
    utc_now_str,
)
from src.common.spark_session import get_spark  # noqa: E402

LAYER = "gold"

UNKNOWN_KEY = -1
NO_CAMPAIGN_KEY = 0


def _silver(name: str) -> DataFrame:
    return get_spark().read.format("delta").load(SILVER_TABLES[name])


def _is_delta(path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(get_spark(), path)


# ---------------------------------------------------------------------------
# DimDate
# ---------------------------------------------------------------------------

def build_dim_date(run_id: str) -> int:
    """Static calendar. Rebuilt wholesale — it is tiny and has no source system.

    date_key is an integer yyyyMMdd rather than an arbitrary sequence. It joins
    as fast as any surrogate, sorts chronologically, and is readable in a query
    result, which makes debugging a join far easier.
    """
    spark = get_spark()
    df = (
        spark.sql(
            f"SELECT explode(sequence(to_date('{DIM_DATE_START}'), "
            f"to_date('{DIM_DATE_END}'), interval 1 day)) AS date"
        )
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("year_month", F.date_format("date", "yyyyMM").cast("int"))
        .withColumn("year_month_label", F.date_format("date", "yyyy-MM"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .select(
            "date_key", "date", "year", "quarter", "month", "month_name",
            "year_month", "year_month_label", "day_of_month", "day_of_week",
            "day_name", "week_of_year", "is_weekend",
        )
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        GOLD_TABLES["dim_date"]
    )
    return df.count()


# ---------------------------------------------------------------------------
# Surrogate key assignment
# ---------------------------------------------------------------------------

def _assign_surrogate_keys(
    incoming: DataFrame, dim_path: str, key_col: str, natural_col: str
) -> DataFrame:
    """Give each *new* natural key the next free surrogate; reuse existing ones.

    Reusing the existing key for a natural key already in the dimension is the
    whole point — it is what keeps previously loaded facts pointing at the
    correct dimension row after a reload.
    """
    spark = get_spark()
    if _is_delta(dim_path):
        existing = spark.read.format("delta").load(dim_path).select(key_col, natural_col)
        max_key = existing.agg(F.max(key_col)).collect()[0][0] or 0
        max_key = max(int(max_key), 0)
        joined = incoming.join(existing, on=natural_col, how="left")
        new_rows = joined.where(F.col(key_col).isNull()).drop(key_col)
        known = joined.where(F.col(key_col).isNotNull())
    else:
        max_key = 0
        new_rows = incoming
        known = None

    w = Window.orderBy(natural_col)
    assigned = new_rows.withColumn(
        key_col, (F.row_number().over(w) + F.lit(max_key)).cast("int")
    )
    if known is not None:
        assigned = assigned.unionByName(known)
    return assigned


def _merge_dim(
    df: DataFrame, dim_path: str, natural_col: str, key_col: str
) -> tuple[int, int]:
    """SCD Type 1 upsert on the natural key, skipping rows that did not change.

    The change-detection hash matters more than it looks. Without it every run
    updates every dimension row, Delta rewrites the whole file each time, and
    the "rows updated" figure in the audit log becomes meaningless — you can no
    longer tell a real attribute change from routine churn.
    """
    spark = get_spark()
    from delta.tables import DeltaTable

    attrs = [c for c in df.columns if c not in (key_col, "_row_hash")]
    hashed = df.withColumn(
        "_row_hash",
        F.sha2(
            F.concat_ws(
                "||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in attrs]
            ),
            256,
        ),
    )

    if not _is_delta(dim_path):
        hashed.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(dim_path)
        return hashed.count(), 0

    before_df = spark.read.format("delta").load(dim_path)
    before = before_df.count()
    changed = (
        hashed.alias("s")
        .join(before_df.select(natural_col, "_row_hash").alias("t"), on=natural_col, how="inner")
        .where("s._row_hash <> t._row_hash")
        .count()
    )
    (
        DeltaTable.forPath(spark, dim_path)
        .alias("t")
        .merge(hashed.alias("s"), f"t.{natural_col} = s.{natural_col}")
        .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    after = spark.read.format("delta").load(dim_path).count()
    return after - before, changed


# ---------------------------------------------------------------------------
# DimProduct / DimStore / DimCampaign
# ---------------------------------------------------------------------------

def build_dim_product(run_id: str) -> dict:
    src = _silver("products").select(
        "product_id", "product_name", "category", "sub_category", "brand",
        "unit_cost", "list_price", "gross_margin_pct",
    )
    keyed = _assign_surrogate_keys(src, GOLD_TABLES["dim_product"], "product_key", "product_id")

    unknown = get_spark().createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "Unknown Product", "Unknown", "Unknown", "Unknown")],
        "product_key INT, product_id STRING, product_name STRING, "
        "category STRING, sub_category STRING, brand STRING",
    ).withColumn("unit_cost", F.lit(None).cast("decimal(12,2)")) \
     .withColumn("list_price", F.lit(None).cast("decimal(12,2)")) \
     .withColumn("gross_margin_pct", F.lit(None).cast("decimal(6,4)"))

    final = keyed.select(
        "product_key", "product_id", "product_name", "category", "sub_category",
        "brand", "unit_cost", "list_price", "gross_margin_pct",
    ).unionByName(unknown.select(
        "product_key", "product_id", "product_name", "category", "sub_category",
        "brand", "unit_cost", "list_price", "gross_margin_pct",
    ))
    ins, upd = _merge_dim(final, GOLD_TABLES["dim_product"], "product_id", "product_key")
    return {"table": "dim_product", "inserted": ins, "updated": upd}


def build_dim_store(run_id: str) -> dict:
    src = _silver("stores").select(
        "store_id", "store_name", "city", "state", "region", "store_format", "open_date"
    )
    keyed = _assign_surrogate_keys(src, GOLD_TABLES["dim_store"], "store_key", "store_id")

    unknown = get_spark().createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "Unknown Store", "Unknown", "Unknown", "Unknown", "Unknown")],
        "store_key INT, store_id STRING, store_name STRING, city STRING, "
        "state STRING, region STRING, store_format STRING",
    ).withColumn("open_date", F.lit(None).cast("date"))

    cols = ["store_key", "store_id", "store_name", "city", "state", "region",
            "store_format", "open_date"]
    final = keyed.select(*cols).unionByName(unknown.select(*cols))
    ins, upd = _merge_dim(final, GOLD_TABLES["dim_store"], "store_id", "store_key")
    return {"table": "dim_store", "inserted": ins, "updated": upd}


def build_dim_campaign(run_id: str) -> dict:
    """Campaign dimension plus the two members that keep FactSales inner-joinable.

    key  0 → 'NONE'    the sale was genuinely not part of any promotion
    key -1 → 'UNKNOWN' the source supplied a campaign id we could not resolve
    Keeping these apart matters: "not promoted" and "we don't know" are
    different answers, and collapsing them would quietly inflate baseline sales.
    """
    spark = get_spark()
    src = _silver("campaigns").select(
        "campaign_id", "campaign_name", "product_id", "start_date", "end_date",
        "discount_percentage", "campaign_cost", "duration_days",
    )
    # Surrogates start above the reserved members.
    keyed = _assign_surrogate_keys(src, GOLD_TABLES["dim_campaign"], "campaign_key", "campaign_id")
    keyed = keyed.withColumn(
        "campaign_key",
        F.when(F.col("campaign_key") > 0, F.col("campaign_key")).otherwise(F.col("campaign_key")),
    )

    specials = spark.createDataFrame(
        [
            (NO_CAMPAIGN_KEY, "NONE", "No Campaign", "N/A"),
            (UNKNOWN_KEY, "UNKNOWN", "Unknown Campaign", "N/A"),
        ],
        "campaign_key INT, campaign_id STRING, campaign_name STRING, product_id STRING",
    ).withColumn("start_date", F.lit(None).cast("date")) \
     .withColumn("end_date", F.lit(None).cast("date")) \
     .withColumn("discount_percentage", F.lit(None).cast("decimal(5,2)")) \
     .withColumn("campaign_cost", F.lit(None).cast("decimal(12,2)")) \
     .withColumn("duration_days", F.lit(None).cast("int"))

    cols = ["campaign_key", "campaign_id", "campaign_name", "product_id",
            "start_date", "end_date", "discount_percentage", "campaign_cost",
            "duration_days"]
    final = keyed.select(*cols).unionByName(specials.select(*cols))
    ins, upd = _merge_dim(final, GOLD_TABLES["dim_campaign"], "campaign_id", "campaign_key")
    return {"table": "dim_campaign", "inserted": ins, "updated": upd}


# ---------------------------------------------------------------------------
# FactSales
# ---------------------------------------------------------------------------

def build_fact_sales(batch_id: str, run_id: str, full_reload: bool = False) -> dict:
    """Incrementally MERGE Silver sales into the fact table.

    Reads only Silver rows above the Gold watermark, resolves the four foreign
    keys, and upserts on transaction_id. Because the fact is keyed on the
    transaction, a corrected transaction *replaces* its earlier version instead
    of being counted twice.
    """
    spark = get_spark()
    from delta.tables import DeltaTable

    started = time.time()
    started_at = utc_now_str()
    path = GOLD_TABLES["fact_sales"]
    watermark_from = "1900-01-01 00:00:00" if full_reload else get_watermark(LAYER, "fact_sales")

    silver = _silver("sales").where(F.col("last_modified_timestamp") > F.lit(watermark_from))
    rows_read = silver.count()
    if rows_read == 0:
        log_run(run_id=run_id, batch_id=batch_id, layer=LAYER, table_name="fact_sales",
                status="SUCCESS_NO_DATA", rows_read=0, watermark_from=watermark_from,
                watermark_to=watermark_from, started_at=started_at,
                finished_at=utc_now_str(), duration_seconds=round(time.time() - started, 3),
                message="No Silver rows above the Gold watermark.")
        return {"table": "fact_sales", "rows_read": 0, "inserted": 0, "updated": 0}

    dim_product = spark.read.format("delta").load(GOLD_TABLES["dim_product"]).select(
        "product_key", "product_id", "unit_cost"
    )
    dim_store = spark.read.format("delta").load(GOLD_TABLES["dim_store"]).select(
        "store_key", "store_id"
    )
    dim_campaign = spark.read.format("delta").load(GOLD_TABLES["dim_campaign"]).select(
        "campaign_key", "campaign_id"
    )

    # Left joins + coalesce to the Unknown member: a missing dimension row can
    # never drop a fact row, and can never leave a NULL foreign key behind.
    fact = (
        silver.join(F.broadcast(dim_product), on="product_id", how="left")
        .join(F.broadcast(dim_store), on="store_id", how="left")
        .join(F.broadcast(dim_campaign), on="campaign_id", how="left")
        .withColumn("product_key", F.coalesce("product_key", F.lit(UNKNOWN_KEY)))
        .withColumn("store_key", F.coalesce("store_key", F.lit(UNKNOWN_KEY)))
        .withColumn("campaign_key", F.coalesce("campaign_key", F.lit(UNKNOWN_KEY)))
        .withColumn("date_key", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
        .withColumn("year_month", F.date_format("transaction_date", "yyyyMM").cast("int"))
        .withColumn("cogs", (F.col("quantity") * F.coalesce("unit_cost", F.lit(0)))
                    .cast("decimal(14,2)"))
        .withColumn("gross_profit", (F.col("net_revenue") - F.col("cogs")).cast("decimal(14,2)"))
        .withColumn("is_promoted", F.col("campaign_key") > F.lit(NO_CAMPAIGN_KEY))
        .select(
            "transaction_id",          # degenerate dimension / primary key
            "date_key", "product_key", "store_key", "campaign_key",
            "transaction_timestamp", "transaction_date", "year_month",
            "quantity", "unit_price", "gross_amount", "discount_amount",
            "net_revenue", "cogs", "gross_profit",
            "is_promoted", "campaign_resolved",
            "last_modified_timestamp",
            F.lit(run_id).alias("_gold_run_id"),
        )
    ).cache()
    rows_prepared = fact.count()

    if _is_delta(path) and not full_reload:
        before = spark.read.format("delta").load(path).count()
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(fact.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedUpdateAll(
                condition="s.last_modified_timestamp > t.last_modified_timestamp"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = spark.read.format("delta").load(path).count()
        inserted = after - before
        updated = rows_prepared - inserted
    else:
        # Partitioned by month: ~4 partitions of ~33K rows each. Partitioning by
        # day would produce 120 files of a few hundred rows — the small-file
        # problem, where per-file overhead costs more than the pruning saves.
        fact.write.format("delta").partitionBy("year_month").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        inserted, updated = rows_prepared, 0

    new_watermark = silver.agg(F.max("last_modified_timestamp")).collect()[0][0]
    set_watermark(LAYER, "fact_sales", "last_modified_timestamp", str(new_watermark),
                  rows_prepared, run_id)

    log_run(run_id=run_id, batch_id=batch_id, layer=LAYER, table_name="fact_sales",
            status="SUCCESS", rows_read=rows_read, rows_written=rows_prepared,
            rows_inserted=inserted, rows_updated=max(0, updated),
            watermark_from=watermark_from, watermark_to=str(new_watermark),
            started_at=started_at, finished_at=utc_now_str(),
            duration_seconds=round(time.time() - started, 3),
            message=f"{inserted} inserted, {max(0, updated)} updated by MERGE.")
    fact.unpersist()
    return {"table": "fact_sales", "rows_read": rows_read, "inserted": inserted,
            "updated": max(0, updated)}


# ---------------------------------------------------------------------------
# FactInventorySnapshot
# ---------------------------------------------------------------------------

def build_fact_inventory(batch_id: str, run_id: str, full_reload: bool = False) -> dict:
    spark = get_spark()
    from delta.tables import DeltaTable

    started = time.time()
    started_at = utc_now_str()
    path = GOLD_TABLES["fact_inventory_snapshot"]
    watermark_from = (
        "1900-01-01 00:00:00" if full_reload else get_watermark(LAYER, "fact_inventory_snapshot")
    )

    silver = _silver("inventory").where(F.col("last_modified_timestamp") > F.lit(watermark_from))
    rows_read = silver.count()
    if rows_read == 0:
        log_run(run_id=run_id, batch_id=batch_id, layer=LAYER,
                table_name="fact_inventory_snapshot", status="SUCCESS_NO_DATA",
                rows_read=0, watermark_from=watermark_from, watermark_to=watermark_from,
                started_at=started_at, finished_at=utc_now_str(),
                duration_seconds=round(time.time() - started, 3),
                message="No Silver rows above the Gold watermark.")
        return {"table": "fact_inventory_snapshot", "rows_read": 0, "inserted": 0, "updated": 0}

    dim_product = spark.read.format("delta").load(GOLD_TABLES["dim_product"]).select(
        "product_key", "product_id", "unit_cost"
    )
    dim_store = spark.read.format("delta").load(GOLD_TABLES["dim_store"]).select(
        "store_key", "store_id"
    )

    fact = (
        silver.join(F.broadcast(dim_product), on="product_id", how="left")
        .join(F.broadcast(dim_store), on="store_id", how="left")
        .withColumn("product_key", F.coalesce("product_key", F.lit(UNKNOWN_KEY)))
        .withColumn("store_key", F.coalesce("store_key", F.lit(UNKNOWN_KEY)))
        .withColumn("date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
        .withColumn("year_month", F.date_format("snapshot_date", "yyyyMM").cast("int"))
        # Valuing stock at cost (not retail) is what makes it comparable with
        # COGS in the turnover ratio — mixing the two would inflate the divisor.
        .withColumn(
            "stock_value_at_cost",
            (F.col("stock_quantity") * F.coalesce("unit_cost", F.lit(0))).cast("decimal(16,2)"),
        )
        .select(
            "date_key", "product_key", "store_key", "snapshot_date", "year_month",
            "inventory_id", "stock_quantity", "stock_value_at_cost",
            "last_modified_timestamp", F.lit(run_id).alias("_gold_run_id"),
        )
    ).cache()
    rows_prepared = fact.count()

    if _is_delta(path) and not full_reload:
        before = spark.read.format("delta").load(path).count()
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(
                fact.alias("s"),
                "t.date_key = s.date_key AND t.product_key = s.product_key "
                "AND t.store_key = s.store_key",
            )
            .whenMatchedUpdateAll(
                condition="s.last_modified_timestamp > t.last_modified_timestamp"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = spark.read.format("delta").load(path).count()
        inserted = after - before
        updated = rows_prepared - inserted
    else:
        fact.write.format("delta").partitionBy("year_month").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        inserted, updated = rows_prepared, 0

    new_watermark = silver.agg(F.max("last_modified_timestamp")).collect()[0][0]
    set_watermark(LAYER, "fact_inventory_snapshot", "last_modified_timestamp",
                  str(new_watermark), rows_prepared, run_id)
    log_run(run_id=run_id, batch_id=batch_id, layer=LAYER,
            table_name="fact_inventory_snapshot", status="SUCCESS", rows_read=rows_read,
            rows_written=rows_prepared, rows_inserted=inserted, rows_updated=max(0, updated),
            watermark_from=watermark_from, watermark_to=str(new_watermark),
            started_at=started_at, finished_at=utc_now_str(),
            duration_seconds=round(time.time() - started, 3),
            message=f"{inserted} inserted, {max(0, updated)} updated by MERGE.")
    fact.unpersist()
    return {"table": "fact_inventory_snapshot", "rows_read": rows_read,
            "inserted": inserted, "updated": max(0, updated)}


# ---------------------------------------------------------------------------
# Optimisation
# ---------------------------------------------------------------------------

def optimize_gold() -> None:
    """Compact small files and cluster the facts on their most-filtered keys.

    Each incremental MERGE leaves behind new small files. Left alone, the fact
    table degrades into hundreds of tiny files and every scan pays per-file
    open cost. OPTIMIZE rewrites them into a few large ones; ZORDER co-locates
    rows by store and product so a filtered query reads fewer files.
    """
    spark = get_spark()
    from delta.tables import DeltaTable

    # The DeltaTable API is used rather than `OPTIMIZE delta.`<path>`` SQL on
    # purpose. That SQL form resolves the path through spark_catalog, which on a
    # Unity Catalog workspace with legacy access disabled raises
    # UC_HIVE_METASTORE_DISABLED_EXCEPTION. The Python API addresses the path
    # directly and works on both a local session and a UC-enabled cluster.
    for path, zorder in (
        (GOLD_TABLES["fact_sales"], ["store_key", "product_key", "campaign_key"]),
        (GOLD_TABLES["fact_inventory_snapshot"], ["store_key", "product_key"]),
    ):
        if _is_delta(path):
            DeltaTable.forPath(spark, path).optimize().executeZOrderBy(*zorder)
    for name in ("dim_product", "dim_store", "dim_campaign", "dim_date"):
        p = GOLD_TABLES[name]
        if _is_delta(p):
            DeltaTable.forPath(spark, p).optimize().executeCompaction()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_gold(batch_id: str, run_id: str, full_reload: bool = False) -> list[dict]:
    results = []

    n_dates = build_dim_date(run_id)
    print(f"    gold/dim_date         rows={n_dates:,}")
    results.append({"table": "dim_date", "rows": n_dates})

    for fn in (build_dim_product, build_dim_store, build_dim_campaign):
        r = fn(run_id)
        results.append(r)
        print(f"    gold/{r['table']:<17} inserted={r['inserted']:>5,} updated={r['updated']:>5,}")

    for fn in (build_fact_sales, build_fact_inventory):
        r = fn(batch_id, run_id, full_reload=full_reload)
        results.append(r)
        print(
            f"    gold/{r['table']:<17} read={r['rows_read']:>7,} "
            f"inserted={r['inserted']:>7,} updated={r['updated']:>6,}"
        )

    optimize_gold()
    print("    gold/OPTIMIZE + ZORDER complete")
    return results
