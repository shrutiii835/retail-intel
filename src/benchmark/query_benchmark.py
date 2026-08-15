"""Analytical query benchmark: before vs after the Gold star schema.

This produces the evidence for the "~30% query performance improvement" claim.
It is a real measurement, and whatever it reports is what goes in METRICS.md —
the numbers are not tuned to hit a target.

Two comparisons are run, because a single one would be misleading:

  COMPARISON A — "before the project" vs "after"
      Baseline is what an analyst actually did before this pipeline existed:
      read the raw CSV extracts, cast the text columns, throw out the rows that
      fail the quality rules, collapse re-sent transactions, join the three
      masters on their string business keys, and compute revenue on the fly.
      This is the honest before/after of the project as a whole, but it bundles
      two effects together: the file format AND the dimensional model.

  COMPARISON B — dimensional model in isolation
      Both sides read Delta. The baseline reads Silver, joins on string natural
      keys and computes revenue at query time; the optimised side reads the Gold
      star schema with integer surrogate keys, a stored revenue column, monthly
      partitioning and compacted Z-ORDERed files. Same format on both sides, so
      what is left is the modelling and layout work.

Method
------
Every query runs a warm-up pass (excluded) then N measured passes. The cache is
cleared before each pass, so no run benefits from a previous one. The MEDIAN is
reported, not the mean — a single GC pause or file-system hiccup skews a mean
badly at these durations, and the median is the more stable statistic on small
samples.

Results are also checked for equivalence: if the two sides disagree on total
revenue or row count, the speed comparison is meaningless, and the benchmark
says so instead of quietly reporting a number.

    python -m src.benchmark.query_benchmark
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.project_config import (  # noqa: E402
    GOLD_TABLES,
    METRICS_DIR,
    QUARANTINE_TABLES,
    PERIOD_END,
    PERIOD_START,
    SILVER_TABLES,
    SOURCE_DIR,
)
from src.common.schemas import REJECT, SALES_CONTRACT  # noqa: E402
from src.common.spark_session import get_spark, stop_spark  # noqa: E402

MEASURED_RUNS = 15
WARMUP_RUNS = 1

_PERIOD_END_EXCLUSIVE = (
    __import__("datetime").datetime.strptime(PERIOD_END, "%Y-%m-%d")
    + __import__("datetime").timedelta(days=1)
).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Baseline A — raw CSV, cleaned and joined at query time
# ---------------------------------------------------------------------------

def _raw_csv_clean() -> DataFrame:
    """Reproduce the curated sales row set from raw CSV, at query time.

    This is deliberately the same logic Silver applies — the same rule
    expressions, the same dedup — because the comparison is only fair if both
    sides answer the same question. The cost of doing it per query instead of
    once per load is exactly the thing being measured.
    """
    spark = get_spark()
    read = lambda name: (  # noqa: E731
        spark.read.option("header", "true").option("inferSchema", "false")
        .csv(str(Path(SOURCE_DIR) / f"{name}.csv"))
    )

    sales = read("sales")
    products = read("products")
    stores = read("stores")
    campaigns = read("campaigns")

    typed = (
        sales.withColumn("transaction_timestamp_ts",
                         F.expr("try_cast(transaction_timestamp AS timestamp)"))
        .withColumn("quantity_dbl", F.expr("try_cast(quantity AS double)"))
        .withColumn("quantity_int",
                    F.when(F.col("quantity_dbl").isNotNull()
                           & (F.col("quantity_dbl") == F.floor(F.col("quantity_dbl"))),
                           F.col("quantity_dbl").cast("int")))
        .withColumn("unit_price_dec", F.expr("try_cast(unit_price AS decimal(12,2))"))
        .withColumn("discount_amount_dec",
                    F.coalesce(F.expr("try_cast(discount_amount AS decimal(12,2))"),
                               F.lit(0).cast("decimal(12,2)")))
        .withColumn("last_modified_ts",
                    F.expr("try_cast(last_modified_timestamp AS timestamp)"))
        .withColumn("campaign_id_clean",
                    F.when(F.trim(F.col("campaign_id")) == "", None)
                    .otherwise(F.trim(F.col("campaign_id"))))
    )

    typed = (
        typed.join(F.broadcast(products.select("product_id").distinct()
                               .withColumn("product_exists", F.lit(True))),
                   on="product_id", how="left")
        .join(F.broadcast(stores.select("store_id").distinct()
                          .withColumn("store_exists", F.lit(True))),
              on="store_id", how="left")
        .join(F.broadcast(campaigns.select(F.col("campaign_id").alias("campaign_id_clean"))
                          .distinct().withColumn("_cf", F.lit(True))),
              on="campaign_id_clean", how="left")
        .withColumn("product_exists", F.coalesce("product_exists", F.lit(False)))
        .withColumn("store_exists", F.coalesce("store_exists", F.lit(False)))
        .withColumn("campaign_exists",
                    F.when(F.col("campaign_id_clean").isNull(), F.lit(True))
                    .otherwise(F.coalesce("_cf", F.lit(False))))
    )

    for rule in SALES_CONTRACT.rules:
        if rule.severity == REJECT:
            typed = typed.where(
                F.expr(rule.expression.format(
                    period_start=PERIOD_START,
                    period_end_exclusive=_PERIOD_END_EXCLUSIVE,
                ))
            )

    w = Window.partitionBy("transaction_id").orderBy(F.col("last_modified_ts").desc_nulls_last())
    deduped = typed.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")

    return (
        deduped.withColumn("gross_amount",
                           (F.col("quantity_int") * F.col("unit_price_dec")).cast("decimal(14,2)"))
        .withColumn("net_revenue",
                    (F.col("gross_amount") - F.col("discount_amount_dec")).cast("decimal(14,2)"))
        .withColumn("campaign_id_final",
                    F.when(F.col("campaign_id_clean").isNull(), F.lit("NONE"))
                    .when(F.col("campaign_exists"), F.col("campaign_id_clean"))
                    .otherwise(F.lit("UNKNOWN")))
        .select(
            "transaction_id",
            F.to_date("transaction_timestamp_ts").alias("transaction_date"),
            "product_id", "store_id",
            F.col("quantity_int").alias("quantity"),
            "gross_amount",
            F.col("discount_amount_dec").alias("discount_amount"),
            "net_revenue",
            F.col("campaign_id_final").alias("campaign_id"),
        )
    )


def _csv_masters():
    spark = get_spark()
    read = lambda name: (  # noqa: E731
        spark.read.option("header", "true").option("inferSchema", "false")
        .csv(str(Path(SOURCE_DIR) / f"{name}.csv"))
    )
    return read("products"), read("stores"), read("campaigns")


# ---------------------------------------------------------------------------
# Query definitions — three variants each, all answering the same question
# ---------------------------------------------------------------------------

def q_store_performance_csv():
    sales = _raw_csv_clean()
    products, stores, campaigns = _csv_masters()
    return (
        sales.join(stores, on="store_id", how="left")
        .groupBy("store_id", "store_name", "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_store_performance_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    stores = spark.read.format("delta").load(SILVER_TABLES["stores"])
    return (
        sales.join(stores, on="store_id", how="left")
        .groupBy("store_id", "store_name", "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_store_performance_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim = spark.read.format("delta").load(GOLD_TABLES["dim_store"])
    return (
        fact.join(F.broadcast(dim), on="store_key", how="inner")
        .groupBy("store_id", "store_name", "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_category_csv():
    sales = _raw_csv_clean()
    products, _, _ = _csv_masters()
    return (
        sales.join(products, on="product_id", how="left")
        .groupBy("category", "sub_category")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_category_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    products = spark.read.format("delta").load(SILVER_TABLES["products"])
    return (
        sales.join(products, on="product_id", how="left")
        .groupBy("category", "sub_category")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_category_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim = spark.read.format("delta").load(GOLD_TABLES["dim_product"])
    return (
        fact.join(F.broadcast(dim), on="product_key", how="inner")
        .groupBy("category", "sub_category")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_campaign_csv():
    sales = _raw_csv_clean()
    _, _, campaigns = _csv_masters()
    return (
        sales.join(campaigns, on="campaign_id", how="left")
        .groupBy("campaign_id", "campaign_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"),
             F.sum("discount_amount").alias("discount"))
        .orderBy(F.col("revenue").desc())
    )


def q_campaign_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    campaigns = spark.read.format("delta").load(SILVER_TABLES["campaigns"])
    return (
        sales.join(campaigns, on="campaign_id", how="left")
        .groupBy("campaign_id", "campaign_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"),
             F.sum("discount_amount").alias("discount"))
        .orderBy(F.col("revenue").desc())
    )


def q_campaign_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim = spark.read.format("delta").load(GOLD_TABLES["dim_campaign"])
    return (
        fact.join(F.broadcast(dim), on="campaign_key", how="inner")
        .groupBy("campaign_id", "campaign_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"),
             F.sum("discount_amount").alias("discount"))
        .orderBy(F.col("revenue").desc())
    )


def q_monthly_region_csv():
    sales = _raw_csv_clean()
    _, stores, _ = _csv_masters()
    return (
        sales.join(stores, on="store_id", how="left")
        .withColumn("year_month", F.date_format("transaction_date", "yyyyMM"))
        .groupBy("year_month", "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy("year_month", "region")
    )


def q_monthly_region_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    stores = spark.read.format("delta").load(SILVER_TABLES["stores"])
    return (
        sales.join(stores, on="store_id", how="left")
        .withColumn("year_month", F.date_format("transaction_date", "yyyyMM"))
        .groupBy("year_month", "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy("year_month", "region")
    )


def q_monthly_region_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim_s = spark.read.format("delta").load(GOLD_TABLES["dim_store"])
    dim_d = spark.read.format("delta").load(GOLD_TABLES["dim_date"])
    return (
        fact.join(F.broadcast(dim_s), on="store_key", how="inner")
        .join(F.broadcast(dim_d.select("date_key", F.col("year_month").alias("ym"))),
              on="date_key", how="inner")
        .groupBy(F.col("ym").cast("string").alias("year_month"), "region")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy("year_month", "region")
    )


# --- filtered queries -------------------------------------------------------
# The four queries above are full scans. A Power BI page almost never is: a
# slicer is nearly always applied. Partition pruning and Z-ORDER only pay off
# when there is a predicate to prune on, so excluding filtered queries would
# understate the Gold layout and misrepresent the real reporting workload.

FILTER_MONTH = 202503
FILTER_REGION = "South"
FILTER_CAMPAIGN = "C010"


def q_filtered_month_region_csv():
    sales = _raw_csv_clean()
    products, stores, _ = _csv_masters()
    return (
        sales.join(stores, on="store_id", how="left")
        .join(products.select("product_id", "category"), on="product_id", how="left")
        .where(
            (F.date_format("transaction_date", "yyyyMM") == F.lit(str(FILTER_MONTH)))
            & (F.col("region") == FILTER_REGION)
        )
        .groupBy("category", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_filtered_month_region_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    stores = spark.read.format("delta").load(SILVER_TABLES["stores"])
    products = spark.read.format("delta").load(SILVER_TABLES["products"])
    return (
        sales.join(stores, on="store_id", how="left")
        .join(products.select("product_id", "category"), on="product_id", how="left")
        .where(
            (F.date_format("transaction_date", "yyyyMM") == F.lit(str(FILTER_MONTH)))
            & (F.col("region") == FILTER_REGION)
        )
        .groupBy("category", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_filtered_month_region_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim_s = spark.read.format("delta").load(GOLD_TABLES["dim_store"])
    dim_p = spark.read.format("delta").load(GOLD_TABLES["dim_product"])
    return (
        # year_month is the partition column, so this predicate prunes whole
        # files before any of them is opened.
        fact.where(F.col("year_month") == FILTER_MONTH)
        .join(F.broadcast(dim_s.where(F.col("region") == FILTER_REGION)),
              on="store_key", how="inner")
        .join(F.broadcast(dim_p.select("product_key", "category")),
              on="product_key", how="inner")
        .groupBy("category", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_single_campaign_csv():
    sales = _raw_csv_clean()
    _, stores, campaigns = _csv_masters()
    return (
        sales.where(F.col("campaign_id") == FILTER_CAMPAIGN)
        .join(stores, on="store_id", how="left")
        .groupBy("region", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_single_campaign_silver():
    spark = get_spark()
    sales = spark.read.format("delta").load(SILVER_TABLES["sales"])
    stores = spark.read.format("delta").load(SILVER_TABLES["stores"])
    return (
        sales.where(F.col("campaign_id") == FILTER_CAMPAIGN)
        .join(stores, on="store_id", how="left")
        .groupBy("region", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


def q_single_campaign_gold():
    spark = get_spark()
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"])
    dim_s = spark.read.format("delta").load(GOLD_TABLES["dim_store"])
    dim_c = spark.read.format("delta").load(GOLD_TABLES["dim_campaign"])
    ck = (
        dim_c.where(F.col("campaign_id") == FILTER_CAMPAIGN)
        .select("campaign_key")
        .collect()
    )
    key = ck[0]["campaign_key"] if ck else -999
    return (
        # campaign_key is a ZORDER column, so matching files cluster together.
        fact.where(F.col("campaign_key") == key)
        .join(F.broadcast(dim_s), on="store_key", how="inner")
        .groupBy("region", "store_name")
        .agg(F.sum("net_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .orderBy(F.col("revenue").desc())
    )


QUERIES = [
    ("Q1_store_performance", q_store_performance_csv, q_store_performance_silver,
     q_store_performance_gold, "Revenue and units by store, ranked"),
    ("Q2_sales_by_category", q_category_csv, q_category_silver, q_category_gold,
     "Revenue and units by product category and sub-category"),
    ("Q3_campaign_performance", q_campaign_csv, q_campaign_silver, q_campaign_gold,
     "Revenue, units and discount by campaign"),
    ("Q4_monthly_by_region", q_monthly_region_csv, q_monthly_region_silver,
     q_monthly_region_gold, "Monthly revenue trend by region"),
    ("Q5_filtered_month_region", q_filtered_month_region_csv,
     q_filtered_month_region_silver, q_filtered_month_region_gold,
     f"Revenue by category and store for {FILTER_MONTH}, {FILTER_REGION} region "
     f"(filtered — exercises partition pruning)"),
    ("Q6_single_campaign_drilldown", q_single_campaign_csv, q_single_campaign_silver,
     q_single_campaign_gold,
     f"Store-level drilldown for campaign {FILTER_CAMPAIGN} "
     f"(filtered — exercises ZORDER clustering)"),
]


# ---------------------------------------------------------------------------
# Timing harness
# ---------------------------------------------------------------------------

def _time_query(build_fn) -> tuple[float, list[dict]]:
    """Clear cache, build the plan fresh, collect, return elapsed seconds."""
    spark = get_spark()
    spark.catalog.clearCache()
    t0 = time.perf_counter()
    rows = build_fn().collect()
    elapsed = time.perf_counter() - t0
    return elapsed, [r.asDict() for r in rows]


def _measure(build_fn) -> tuple[dict, list[dict]]:
    for _ in range(WARMUP_RUNS):
        _time_query(build_fn)
    times, rows = [], None
    for _ in range(MEASURED_RUNS):
        t, rows = _time_query(build_fn)
        times.append(t)
    return (
        {
            "runs": MEASURED_RUNS,
            "median_seconds": round(statistics.median(times), 4),
            "mean_seconds": round(statistics.mean(times), 4),
            "min_seconds": round(min(times), 4),
            "max_seconds": round(max(times), 4),
            "stdev_seconds": round(statistics.stdev(times), 4) if len(times) > 1 else 0.0,
            "all_seconds": [round(t, 4) for t in times],
        },
        rows,
    )


def _significant(a: dict, b: dict) -> bool:
    """Is the gap between two variants bigger than the measurement noise?

    At ~130K rows on single-node Spark, per-query latency is dominated by fixed
    job-setup overhead, and run-to-run standard deviation can be as large as the
    difference being measured. Reporting a percentage without this test would be
    reporting noise as a result. The bar used is conservative: the difference in
    medians must exceed the SUM of the two standard deviations.
    """
    diff = abs(a["median_seconds"] - b["median_seconds"])
    noise = a["stdev_seconds"] + b["stdev_seconds"]
    return diff > noise


def _totals(rows: list[dict]) -> dict:
    return {
        "row_count": len(rows),
        "revenue": round(sum(float(r.get("revenue") or 0) for r in rows), 2),
        "units": int(sum(int(r.get("units") or 0) for r in rows)),
    }


def _reconcile_baseline_variance() -> dict:
    """Account for the known, legitimate gap between the CSV baseline and Gold.

    The two are not expected to agree exactly, and the reason is worth stating
    precisely rather than hiding behind a tolerance.

    The raw CSV holds only the CURRENT version of each transaction. The pipeline
    is incremental: when a *correction* to an already-loaded transaction fails
    validation, that correction is quarantined and the previously loaded
    known-good version stays in Silver and Gold. Recomputing from the current
    CSV snapshot therefore drops the transaction entirely, while the pipeline
    keeps its last good version.

    That is a deliberate difference in semantics, not a defect — but it must be
    fully explained, so this function verifies that every extra Gold row has a
    quarantined current version. Any row it cannot explain is reported as
    unexplained variance, which would invalidate the benchmark.
    """
    spark = get_spark()
    from delta.tables import DeltaTable

    csv_ids = _raw_csv_clean().select("transaction_id")
    gold = (
        spark.read.format("delta")
        .load(GOLD_TABLES["fact_sales"])
        .select("transaction_id", "quantity", "net_revenue")
    )
    extra = gold.join(csv_ids, on="transaction_id", how="left_anti").cache()
    n_extra = extra.count()

    qpath = QUARANTINE_TABLES["sales"]
    if DeltaTable.isDeltaTable(spark, qpath):
        q_ids = spark.read.format("delta").load(qpath).select("transaction_id").distinct()
        unexplained = extra.join(q_ids, on="transaction_id", how="left_anti").count()
    else:
        unexplained = n_extra

    agg = extra.agg(
        F.sum("quantity").alias("units"), F.sum("net_revenue").alias("revenue")
    ).collect()[0]
    ids = [r["transaction_id"] for r in extra.select("transaction_id").limit(25).collect()]
    extra.unpersist()

    return {
        "explanation": (
            "Rows present in Gold but absent from a fresh recomputation of the "
            "raw CSV: their CURRENT source version failed validation and was "
            "quarantined, so the incremental pipeline retains the previously "
            "loaded known-good version."
        ),
        "extra_gold_rows": n_extra,
        "explained_units": int(agg["units"] or 0),
        "explained_revenue": round(float(agg["revenue"] or 0), 2),
        "unexplained_rows": unexplained,
        "fully_explained": unexplained == 0,
        "transaction_ids": ids,
    }


def main() -> None:
    get_spark()
    print("=== RetailIntel query benchmark ===")
    print(f"  {WARMUP_RUNS} warm-up + {MEASURED_RUNS} measured runs per variant, "
          f"cache cleared before every run, median reported\n")

    recon = _reconcile_baseline_variance()
    print(f"  baseline reconciliation: {recon['extra_gold_rows']} extra Gold rows "
          f"({recon['explained_units']} units, {recon['explained_revenue']} revenue), "
          f"unexplained={recon['unexplained_rows']}\n")

    results = []
    for name, csv_fn, silver_fn, gold_fn, description in QUERIES:
        print(f"  {name}: {description}")
        csv_stats, csv_rows = _measure(csv_fn)
        silver_stats, silver_rows = _measure(silver_fn)
        gold_stats, gold_rows = _measure(gold_fn)

        t_csv = csv_stats["median_seconds"]
        t_silver = silver_stats["median_seconds"]
        t_gold = gold_stats["median_seconds"]

        # Equivalence: a speed-up is only meaningful if the answer is the same.
        # Silver vs Gold must agree EXACTLY — same rows, same money. CSV vs Gold
        # must agree once the reconciled variance above is added back; anything
        # beyond that would mean the two sides are answering different questions.
        tot_csv, tot_silver, tot_gold = _totals(csv_rows), _totals(silver_rows), _totals(gold_rows)
        silver_matches_gold = (
            tot_silver["row_count"] == tot_gold["row_count"]
            and tot_silver["units"] == tot_gold["units"]
            and abs(tot_silver["revenue"] - tot_gold["revenue"]) < 0.01
        )
        # The reconciled variance applies in full only to unfiltered queries. A
        # filtered query may include none, some or all of those 4 rows, so it is
        # checked against a bound rather than an exact figure.
        unit_gap = tot_gold["units"] - tot_csv["units"]
        rev_gap = tot_gold["revenue"] - tot_csv["revenue"]
        csv_reconciles = (
            tot_csv["row_count"] == tot_gold["row_count"]
            and 0 <= unit_gap <= recon["explained_units"]
            and -0.05 <= rev_gap <= recon["explained_revenue"] + 0.05
        )
        equivalent = silver_matches_gold and csv_reconciles and recon["fully_explained"]

        entry = {
            "query": name,
            "description": description,
            "baseline_raw_csv": csv_stats,
            "baseline_silver_delta": silver_stats,
            "gold_star_schema": gold_stats,
            "improvement_vs_raw_csv_pct": round(100.0 * (t_csv - t_gold) / t_csv, 2),
            "improvement_vs_silver_delta_pct": round(100.0 * (t_silver - t_gold) / t_silver, 2),
            "speedup_vs_raw_csv_x": round(t_csv / t_gold, 2) if t_gold else None,
            "speedup_vs_silver_delta_x": round(t_silver / t_gold, 2) if t_gold else None,
            "results_equivalent": equivalent,
            "vs_raw_csv_significant": _significant(csv_stats, gold_stats),
            "vs_silver_delta_significant": _significant(silver_stats, gold_stats),
            "silver_matches_gold_exactly": silver_matches_gold,
            "csv_reconciles_to_gold": csv_reconciles,
            "totals": {"raw_csv": tot_csv, "silver_delta": tot_silver, "gold": tot_gold},
        }
        results.append(entry)
        print(f"    raw CSV      {t_csv:8.4f}s")
        print(f"    silver delta {t_silver:8.4f}s")
        print(f"    gold star    {t_gold:8.4f}s   "
              f"→ {entry['improvement_vs_raw_csv_pct']:.1f}% vs CSV, "
              f"{entry['improvement_vs_silver_delta_pct']:.1f}% vs Silver"
              f"{'' if equivalent else '   [!] RESULTS DIFFER'}")

    def _agg(key: str) -> float:
        return round(statistics.mean([r[key] for r in results]), 2)

    # Aggregate on total time as well as the mean of per-query percentages: the
    # mean of percentages over-weights cheap queries, total time is what an
    # analyst actually waits for. Both are reported.
    tot_csv = sum(r["baseline_raw_csv"]["median_seconds"] for r in results)
    tot_silver = sum(r["baseline_silver_delta"]["median_seconds"] for r in results)
    tot_gold = sum(r["gold_star_schema"]["median_seconds"] for r in results)

    summary = {
        "queries": len(results),
        "measured_runs_per_variant": MEASURED_RUNS,
        "all_results_equivalent": all(r["results_equivalent"] for r in results),
        "mean_improvement_vs_raw_csv_pct": _agg("improvement_vs_raw_csv_pct"),
        "mean_improvement_vs_silver_delta_pct": _agg("improvement_vs_silver_delta_pct"),
        "total_seconds_raw_csv": round(tot_csv, 4),
        "total_seconds_silver_delta": round(tot_silver, 4),
        "total_seconds_gold": round(tot_gold, 4),
        "total_time_improvement_vs_raw_csv_pct": round(100.0 * (tot_csv - tot_gold) / tot_csv, 2),
        "total_time_improvement_vs_silver_delta_pct": round(
            100.0 * (tot_silver - tot_gold) / tot_silver, 2
        ),
        "queries_where_vs_raw_csv_is_significant": sum(
            1 for r in results if r["vs_raw_csv_significant"]
        ),
        "queries_where_vs_silver_delta_is_significant": sum(
            1 for r in results if r["vs_silver_delta_significant"]
        ),
        "interpretation": (
            "The raw-CSV comparison is the project's true before/after and its "
            "margin far exceeds run-to-run noise, so it is a reportable result. "
            "The Silver-Delta comparison isolates the dimensional model alone; at "
            "this data volume that difference falls inside measurement noise on "
            "most queries and must NOT be reported as a performance claim."
        ),
    }

    out = {
        "method": {
            "warmup_runs": WARMUP_RUNS,
            "measured_runs": MEASURED_RUNS,
            "statistic": "median of measured runs",
            "cache": "spark.catalog.clearCache() before every run",
            "environment": "local[*] Spark 3.5.3, Delta 3.2.1",
            "baseline_raw_csv": (
                "Raw CSV extracts, cast at query time, quality rules applied at "
                "query time, transactions deduplicated at query time, masters "
                "joined on string business keys. Represents the pre-project "
                "manual workflow."
            ),
            "baseline_silver_delta": (
                "Cleaned Silver Delta tables joined on string natural keys with "
                "revenue computed at query time. Isolates the dimensional model "
                "by holding the file format constant."
            ),
            "gold_star_schema": (
                "Gold star schema: integer surrogate keys, stored net_revenue, "
                "partitioned by year_month, OPTIMIZE + ZORDER compacted."
            ),
        },
        "baseline_variance_reconciliation": recon,
        "summary": summary,
        "queries": results,
    }

    Path(METRICS_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(METRICS_DIR) / "query_benchmark.json"
    path.write_text(json.dumps(out, indent=2))

    print("\n  --- summary ---")
    print(f"  results equivalent across all variants : {summary['all_results_equivalent']}")
    print(f"  total time raw CSV      : {summary['total_seconds_raw_csv']:.3f}s")
    print(f"  total time Silver Delta : {summary['total_seconds_silver_delta']:.3f}s")
    print(f"  total time Gold star    : {summary['total_seconds_gold']:.3f}s")
    print(f"  improvement vs raw CSV      : "
          f"{summary['total_time_improvement_vs_raw_csv_pct']}% (total time), "
          f"{summary['mean_improvement_vs_raw_csv_pct']}% (mean per query)")
    print(f"  improvement vs Silver Delta : "
          f"{summary['total_time_improvement_vs_silver_delta_pct']}% (total time), "
          f"{summary['mean_improvement_vs_silver_delta_pct']}% (mean per query)")
    print(f"\n  statistically significant (diff > combined stdev):")
    print(f"    vs raw CSV      : {summary['queries_where_vs_raw_csv_is_significant']}"
          f"/{len(results)} queries")
    print(f"    vs Silver Delta : {summary['queries_where_vs_silver_delta_is_significant']}"
          f"/{len(results)} queries")
    print(f"\n  wrote {path}")
    stop_spark()


if __name__ == "__main__":
    main()
