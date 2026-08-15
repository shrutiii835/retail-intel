"""Silver layer — typing, validation, quarantine, deduplication, MERGE.

Bronze holds raw strings. Silver is where the data becomes trustworthy, in this
fixed order:

    read incrementally (watermark)
        → cast to real types (try_cast: failures become NULL, not exceptions)
        → resolve referential integrity against the master tables
        → evaluate every rule in src/common/schemas.py
        → split: REJECT failures go to quarantine, the rest continue
        → apply REPAIR substitutions (unresolvable campaign → Unknown)
        → deduplicate on the business key, keeping the newest version
        → MERGE into the Silver Delta table
        → record per-rule DQ counts, then advance the watermark

Why deduplicate *before* the MERGE
-----------------------------------
Delta's MERGE raises an error if one target row matches multiple source rows —
it cannot decide which version wins. Our source deliberately re-sends
transactions, so a batch can legitimately contain the same transaction_id twice.
Collapsing to one row per business key first (newest last_modified_timestamp
wins) is what makes the MERGE deterministic instead of a runtime failure.

Why MERGE rather than append
----------------------------
Late corrections. ~1.5% of transactions are amended days after the sale, and
arrive in a later batch carrying their original transaction date. An append
would leave both the original and the correction in the table and every revenue
number would be overstated. MERGE updates in place, keyed on the business key,
so the table always holds exactly one current row per transaction — and running
the same batch twice changes nothing.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.project_config import (  # noqa: E402
    BRONZE_TABLES,
    PERIOD_END,
    PERIOD_START,
    QUARANTINE_TABLES,
    SILVER_TABLES,
)
from src.common.control import (  # noqa: E402
    get_watermark,
    log_dq_results,
    log_run,
    set_watermark,
    utc_now_str,
)
from src.common.schemas import CONTRACTS, REJECT, Rule  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402

LAYER = "silver"

SOURCE_ORDER = ("products", "stores", "campaigns", "sales", "inventory")

UNKNOWN_CAMPAIGN_ID = "UNKNOWN"
NO_CAMPAIGN_ID = "NONE"

# Exclusive upper bound for the in-period rule.
_PERIOD_END_EXCLUSIVE = (
    __import__("datetime").datetime.strptime(PERIOD_END, "%Y-%m-%d")
    + __import__("datetime").timedelta(days=1)
).strftime("%Y-%m-%d")


def _expr(rule: Rule) -> str:
    return rule.expression.format(
        period_start=PERIOD_START,
        period_end_exclusive=_PERIOD_END_EXCLUSIVE,
    )


# ---------------------------------------------------------------------------
# Typed / derived columns per source
# ---------------------------------------------------------------------------
# try_cast is used everywhere a source value could be junk. It yields NULL on an
# unparseable value instead of throwing, which is what lets a bad row be
# *reported* by a named rule rather than aborting the whole job.

def _prepare(source: str, df: DataFrame) -> DataFrame:
    spark = get_spark()

    def ref(name: str, col: str):
        """Distinct key list from an already-built Silver master table."""
        from delta.tables import DeltaTable
        path = SILVER_TABLES[name]
        if not DeltaTable.isDeltaTable(spark, path):
            return spark.createDataFrame([], f"{col} STRING")
        return spark.read.format("delta").load(path).select(col).distinct()

    if source == "sales":
        out = (
            df.withColumn(
                "transaction_timestamp_ts",
                F.expr("try_cast(transaction_timestamp AS timestamp)"),
            )
            # "3.0" is a recoverable integer; "N/A" is not. Casting via double
            # first accepts the former and still nulls the latter.
            .withColumn("quantity_dbl", F.expr("try_cast(quantity AS double)"))
            .withColumn(
                "quantity_int",
                F.when(
                    F.col("quantity_dbl").isNotNull()
                    & (F.col("quantity_dbl") == F.floor(F.col("quantity_dbl"))),
                    F.col("quantity_dbl").cast("int"),
                ),
            )
            .withColumn("unit_price_dec", F.expr("try_cast(unit_price AS decimal(12,2))"))
            .withColumn(
                "discount_amount_dec",
                F.coalesce(
                    F.expr("try_cast(discount_amount AS decimal(12,2))"),
                    F.lit(0).cast("decimal(12,2)"),
                ),
            )
            .withColumn(
                "last_modified_ts", F.expr("try_cast(last_modified_timestamp AS timestamp)")
            )
            .withColumn(
                "campaign_id_clean",
                F.when(F.trim(F.col("campaign_id")) == "", None).otherwise(
                    F.trim(F.col("campaign_id"))
                ),
            )
        )
        out = (
            out.join(
                F.broadcast(ref("products", "product_id").withColumn("product_exists", F.lit(True))),
                on="product_id", how="left",
            )
            .join(
                F.broadcast(ref("stores", "store_id").withColumn("store_exists", F.lit(True))),
                on="store_id", how="left",
            )
            .join(
                F.broadcast(
                    ref("campaigns", "campaign_id")
                    .withColumnRenamed("campaign_id", "campaign_id_clean")
                    .withColumn("_campaign_found", F.lit(True))
                ),
                on="campaign_id_clean", how="left",
            )
        )
        return (
            out.withColumn("product_exists", F.coalesce(F.col("product_exists"), F.lit(False)))
            .withColumn("store_exists", F.coalesce(F.col("store_exists"), F.lit(False)))
            # A blank campaign_id is a legitimate non-promoted sale, so it
            # *passes* SAL_014. Only a non-blank id we cannot resolve fails.
            .withColumn(
                "campaign_exists",
                F.when(F.col("campaign_id_clean").isNull(), F.lit(True)).otherwise(
                    F.coalesce(F.col("_campaign_found"), F.lit(False))
                ),
            )
            .drop("_campaign_found", "quantity_dbl")
        )

    if source == "inventory":
        out = (
            df.withColumn("snapshot_date_dt", F.expr("try_cast(snapshot_date AS date)"))
            .withColumn("stock_quantity_dbl", F.expr("try_cast(stock_quantity AS double)"))
            .withColumn(
                "stock_quantity_int",
                F.when(
                    F.col("stock_quantity_dbl").isNotNull()
                    & (F.col("stock_quantity_dbl") == F.floor(F.col("stock_quantity_dbl"))),
                    F.col("stock_quantity_dbl").cast("int"),
                ),
            )
            .withColumn(
                "last_modified_ts", F.expr("try_cast(last_modified_timestamp AS timestamp)")
            )
        )
        return (
            out.join(
                F.broadcast(ref("products", "product_id").withColumn("product_exists", F.lit(True))),
                on="product_id", how="left",
            )
            .join(
                F.broadcast(ref("stores", "store_id").withColumn("store_exists", F.lit(True))),
                on="store_id", how="left",
            )
            .withColumn("product_exists", F.coalesce(F.col("product_exists"), F.lit(False)))
            .withColumn("store_exists", F.coalesce(F.col("store_exists"), F.lit(False)))
            .drop("stock_quantity_dbl")
        )

    if source == "campaigns":
        out = (
            df.withColumn("start_date_dt", F.expr("try_cast(start_date AS date)"))
            .withColumn("end_date_dt", F.expr("try_cast(end_date AS date)"))
            .withColumn(
                "discount_percentage_dec",
                F.expr("try_cast(discount_percentage AS decimal(5,2))"),
            )
            .withColumn("campaign_cost_dec", F.expr("try_cast(campaign_cost AS decimal(12,2))"))
            .withColumn(
                "last_modified_ts", F.expr("try_cast(last_modified_timestamp AS timestamp)")
            )
        )
        return out.join(
            F.broadcast(ref("products", "product_id").withColumn("product_exists", F.lit(True))),
            on="product_id", how="left",
        ).withColumn("product_exists", F.coalesce(F.col("product_exists"), F.lit(False)))

    if source == "products":
        return (
            df.withColumn("unit_cost_dec", F.expr("try_cast(unit_cost AS decimal(12,2))"))
            .withColumn("list_price_dec", F.expr("try_cast(list_price AS decimal(12,2))"))
            .withColumn(
                "last_modified_ts", F.expr("try_cast(last_modified_timestamp AS timestamp)")
            )
        )

    if source == "stores":
        return df.withColumn("open_date_dt", F.expr("try_cast(open_date AS date)")).withColumn(
            "last_modified_ts", F.expr("try_cast(last_modified_timestamp AS timestamp)")
        )

    raise ValueError(f"unknown source: {source}")


# ---------------------------------------------------------------------------
# Final Silver projections
# ---------------------------------------------------------------------------

def _project(source: str, df: DataFrame, run_id: str) -> DataFrame:
    """Select the clean, typed, business-facing column set."""
    lineage = [
        F.col("_batch_id"),
        F.col("_ingest_run_id"),
        F.lit(run_id).alias("_silver_run_id"),
        F.lit(utc_now_str()).cast("timestamp").alias("_silver_processed_at"),
    ]

    if source == "sales":
        gross = (F.col("quantity_int") * F.col("unit_price_dec")).cast("decimal(14,2)")
        return df.select(
            F.col("transaction_id"),
            F.col("transaction_timestamp_ts").alias("transaction_timestamp"),
            F.to_date("transaction_timestamp_ts").alias("transaction_date"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("quantity_int").alias("quantity"),
            F.col("unit_price_dec").alias("unit_price"),
            F.col("discount_amount_dec").alias("discount_amount"),
            gross.alias("gross_amount"),
            # The single definition of revenue for the whole project. Gold and
            # Power BI carry this forward rather than recomputing it, so the
            # three layers cannot disagree.
            (gross - F.col("discount_amount_dec")).cast("decimal(14,2)").alias("net_revenue"),
            F.col("campaign_id_final").alias("campaign_id"),
            F.col("campaign_resolved"),
            F.col("last_modified_ts").alias("last_modified_timestamp"),
            *lineage,
        )

    if source == "inventory":
        return df.select(
            F.col("inventory_id"),
            F.col("snapshot_date_dt").alias("snapshot_date"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity_int").alias("stock_quantity"),
            F.col("last_modified_ts").alias("last_modified_timestamp"),
            *lineage,
        )

    if source == "campaigns":
        return df.select(
            F.col("campaign_id"),
            F.col("campaign_name"),
            F.col("product_id"),
            F.col("start_date_dt").alias("start_date"),
            F.col("end_date_dt").alias("end_date"),
            F.col("discount_percentage_dec").alias("discount_percentage"),
            F.col("campaign_cost_dec").alias("campaign_cost"),
            F.datediff(F.col("end_date_dt"), F.col("start_date_dt")).cast("int").alias("duration_days_exclusive"),
            (F.datediff(F.col("end_date_dt"), F.col("start_date_dt")) + 1).cast("int").alias("duration_days"),
            F.col("last_modified_ts").alias("last_modified_timestamp"),
            *lineage,
        )

    if source == "products":
        return df.select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("sub_category"),
            F.col("brand"),
            F.col("unit_cost_dec").alias("unit_cost"),
            F.col("list_price_dec").alias("list_price"),
            (
                (F.col("list_price_dec") - F.col("unit_cost_dec"))
                / F.when(F.col("list_price_dec") > 0, F.col("list_price_dec"))
            ).cast("decimal(6,4)").alias("gross_margin_pct"),
            F.col("last_modified_ts").alias("last_modified_timestamp"),
            *lineage,
        )

    if source == "stores":
        return df.select(
            F.col("store_id"),
            F.col("store_name"),
            F.col("city"),
            F.col("state"),
            F.col("region"),
            F.col("store_format"),
            F.col("open_date_dt").alias("open_date"),
            F.col("last_modified_ts").alias("last_modified_timestamp"),
            *lineage,
        )

    raise ValueError(source)


# ---------------------------------------------------------------------------
# Main per-source build
# ---------------------------------------------------------------------------

def build_source(source: str, batch_id: str, run_id: str, full_reload: bool = False) -> dict:
    spark = get_spark()
    from delta.tables import DeltaTable

    contract = CONTRACTS[source]
    started = time.time()
    started_at = utc_now_str()
    bronze_path = BRONZE_TABLES[source]
    silver_path = SILVER_TABLES[source]

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        raise FileNotFoundError(f"Bronze table missing for '{source}' at {bronze_path}")

    watermark_from = "1900-01-01 00:00:00" if full_reload else get_watermark(LAYER, source)
    wm_col = contract.watermark_column

    bronze = spark.read.format("delta").load(bronze_path)
    incoming = bronze.where(F.col(wm_col) > F.lit(watermark_from))
    rows_read = incoming.count()

    if rows_read == 0:
        log_run(
            run_id=run_id, batch_id=batch_id, layer=LAYER, table_name=source,
            status="SUCCESS_NO_DATA", rows_read=0,
            watermark_from=watermark_from, watermark_to=watermark_from,
            started_at=started_at, finished_at=utc_now_str(),
            duration_seconds=round(time.time() - started, 3),
            message="No new Bronze rows above the Silver watermark.",
        )
        return {"source": source, "rows_read": 0, "rows_valid": 0, "rows_rejected": 0,
                "duplicates_removed": 0, "inserted": 0, "updated": 0,
                "status": "SUCCESS_NO_DATA"}

    prepared = _prepare(source, incoming)

    # ---- evaluate every rule into its own boolean column -------------------
    for rule in contract.rules:
        prepared = prepared.withColumn(rule.rule_id, F.expr(_expr(rule)))
    prepared = prepared.cache()

    reject_rules = [r for r in contract.rules if r.severity == REJECT]
    repair_rules = [r for r in contract.rules if r.severity != REJECT]

    # ---- per-rule counts (one pass) ----------------------------------------
    agg = prepared.agg(
        *[
            F.sum(F.when(F.col(r.rule_id) == False, 1).otherwise(0)).alias(f"f_{r.rule_id}")  # noqa: E712
            for r in contract.rules
        ]
    ).collect()[0]

    dq_rows = [
        {
            "run_id": run_id, "batch_id": batch_id, "layer": LAYER,
            "table_name": source, "rule_id": r.rule_id,
            "rule_description": r.description, "severity": r.severity,
            "rows_evaluated": rows_read,
            "rows_failed": int(agg[f"f_{r.rule_id}"] or 0),
            "fail_pct": round(100.0 * float(agg[f"f_{r.rule_id}"] or 0) / rows_read, 6),
            "recorded_at": utc_now_str(),
        }
        for r in contract.rules
    ]
    log_dq_results(dq_rows)

    # ---- split valid / quarantined ----------------------------------------
    failed_expr = F.array_compact(
        F.array(
            *[
                F.when(F.col(r.rule_id) == False, F.lit(r.rule_id))  # noqa: E712
                for r in reject_rules
            ]
        )
    )
    tagged = prepared.withColumn("_dq_failed_rules", failed_expr).withColumn(
        "_dq_is_valid", F.size(F.col("_dq_failed_rules")) == 0
    )

    invalid = tagged.where(~F.col("_dq_is_valid"))
    rows_rejected = invalid.count()

    if rows_rejected > 0:
        # Quarantine keeps the ORIGINAL string values, not the cast ones — the
        # whole point is to be able to see what the source actually sent.
        raw_cols = [c for c in incoming.columns]
        (
            invalid.select(
                *raw_cols,
                F.col("_dq_failed_rules"),
                F.concat_ws(",", F.col("_dq_failed_rules")).alias("_dq_failed_rules_csv"),
                F.lit(run_id).alias("_dq_run_id"),
                F.lit(batch_id).alias("_dq_batch_id"),
                F.lit(utc_now_str()).cast("timestamp").alias("_quarantined_at"),
            )
            .write.format("delta")
            .partitionBy("_dq_batch_id")
            .mode("overwrite")
            .option("replaceWhere", f"_dq_batch_id = '{batch_id}'")
            .option("mergeSchema", "true")
            .save(QUARANTINE_TABLES[source])
        )

    valid = tagged.where(F.col("_dq_is_valid"))

    # ---- REPAIR substitutions ---------------------------------------------
    if source == "sales":
        # SAL_014: an unresolvable campaign id must not delete a real sale.
        # Route it to the Unknown member and keep a flag so the effect on
        # attribution stays measurable rather than hidden.
        valid = valid.withColumn(
            "campaign_resolved",
            F.when(F.col("campaign_id_clean").isNull(), F.lit(True)).otherwise(
                F.col("SAL_014")
            ),
        ).withColumn(
            "campaign_id_final",
            F.when(F.col("campaign_id_clean").isNull(), F.lit(NO_CAMPAIGN_ID))
            .when(F.col("SAL_014") == True, F.col("campaign_id_clean"))  # noqa: E712
            .otherwise(F.lit(UNKNOWN_CAMPAIGN_ID)),
        )
    for _r in repair_rules:
        pass  # other sources currently define no repair rules

    rows_valid = valid.count()

    # ---- deduplicate on the business key ----------------------------------
    key = list(contract.business_key)
    w = Window.partitionBy(*key).orderBy(
        F.col("last_modified_ts").desc_nulls_last(),
        F.col("_ingest_timestamp").desc_nulls_last(),
    )
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    projected = _project(source, deduped, run_id).cache()
    rows_after_dedup = projected.count()
    duplicates_removed = rows_valid - rows_after_dedup

    # ---- MERGE -------------------------------------------------------------
    inserted = updated = 0
    if DeltaTable.isDeltaTable(spark, silver_path) and not full_reload:
        target = DeltaTable.forPath(spark, silver_path)
        before = spark.read.format("delta").load(silver_path).count()
        cond = " AND ".join(f"t.{k} = s.{k}" for k in key)
        (
            target.alias("t")
            .merge(projected.alias("s"), cond)
            # Only overwrite when the incoming row is genuinely newer. Without
            # this, replaying an old batch would revert a later correction.
            .whenMatchedUpdateAll(
                condition="s.last_modified_timestamp > t.last_modified_timestamp"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = spark.read.format("delta").load(silver_path).count()
        inserted = after - before
        updated = rows_after_dedup - inserted
    else:
        projected.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(silver_path)
        inserted = rows_after_dedup

    new_watermark = incoming.agg(F.max(wm_col)).collect()[0][0]
    set_watermark(LAYER, source, wm_col, str(new_watermark), rows_after_dedup, run_id)

    duration = time.time() - started
    log_run(
        run_id=run_id, batch_id=batch_id, layer=LAYER, table_name=source,
        status="SUCCESS", rows_read=rows_read, rows_written=rows_after_dedup,
        rows_inserted=inserted, rows_updated=max(0, updated),
        rows_rejected=rows_rejected, rows_duplicate=duplicates_removed,
        watermark_from=watermark_from, watermark_to=str(new_watermark),
        started_at=started_at, finished_at=utc_now_str(),
        duration_seconds=round(duration, 3),
        message=f"{rows_valid} valid, {rows_rejected} quarantined, "
                f"{duplicates_removed} duplicates collapsed.",
    )

    prepared.unpersist()
    projected.unpersist()

    return {
        "source": source,
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "duplicates_removed": duplicates_removed,
        "inserted": inserted,
        "updated": max(0, updated),
        "status": "SUCCESS",
    }


def build_silver(batch_id: str, run_id: str, full_reload: bool = False) -> list[dict]:
    results = []
    for source in SOURCE_ORDER:
        stats = build_source(source, batch_id, run_id, full_reload=full_reload)
        results.append(stats)
        print(
            f"    silver/{source:<10} read={stats['rows_read']:>7,} "
            f"valid={stats['rows_valid']:>7,} rejected={stats['rows_rejected']:>5,} "
            f"dupes={stats['duplicates_removed']:>5,} "
            f"ins={stats['inserted']:>7,} upd={stats['updated']:>5,}"
        )
    return results
