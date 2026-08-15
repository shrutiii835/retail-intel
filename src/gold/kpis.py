"""Gold KPI mart — promotion ROI with a pre/post baseline.

Only ONE aggregate table is built here, deliberately. Store performance and
inventory turnover are left to Power BI measures over the star schema, because
that is what a star schema is for — pre-aggregating them would add tables to
maintain and freeze the grain the business can slice at.

Promotion ROI is the exception. It needs a baseline drawn from a window that
sits *outside* whatever the report is filtered to (the 28 days before the
campaign started). Expressing "look outside the current filter context, but
only for this campaign's product, and only for its own pre-window" in DAX is
possible but fragile, and it recomputes on every slicer click. Computing it once
in Spark, where the window is explicit, is both faster and far easier to defend.

Methodology (assumptions are stated in METRICS.md)
--------------------------------------------------
    baseline_daily_revenue   = revenue for the promoted product over the 28 days
                               immediately before the campaign, ÷ 28
    expected_baseline_revenue = baseline_daily_revenue × campaign duration in days
    incremental_revenue      = actual campaign-period revenue − expected_baseline_revenue
    roi                      = (incremental_revenue − campaign_cost) ÷ campaign_cost

Two things this baseline does NOT claim to do, and METRICS.md says so plainly:

  * It does not separate genuine incremental demand from *pull-forward* — a
    customer who would have bought next week buying now instead. A true read
    needs a control group of stores, which synthetic data cannot supply.
  * It attributes all of the period's change to the campaign. Seasonality and
    payday effects live in the same window and are not netted out.

A revenue-based ROI also ignores the margin given away by the discount, so a
margin-based variant is computed alongside it. The margin figure is the more
honest one for a commercial decision; both are exposed so the difference is
visible rather than hidden.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from config.project_config import BASELINE_WINDOW_DAYS, GOLD_TABLES  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402

MART_CAMPAIGN_ROI = f"{GOLD_TABLES['fact_sales'].rsplit('/', 1)[0]}/mart_campaign_roi"


def build_campaign_roi_mart() -> dict:
    spark = get_spark()

    dim_campaign = (
        spark.read.format("delta")
        .load(GOLD_TABLES["dim_campaign"])
        # Exclude the reserved members (0 = No Campaign, -1 = Unknown): they
        # have no cost and no window, so an ROI for them is meaningless.
        .where("campaign_key > 0")
        .select(
            "campaign_key", "campaign_id", "campaign_name",
            F.col("product_id").alias("campaign_product_id"),
            "start_date", "end_date", "discount_percentage", "campaign_cost",
            "duration_days",
        )
    )

    dim_product = spark.read.format("delta").load(GOLD_TABLES["dim_product"]).select(
        "product_key", "product_id", "product_name", "category", "brand"
    )
    fact = spark.read.format("delta").load(GOLD_TABLES["fact_sales"]).select(
        "product_key", "transaction_date", "quantity", "net_revenue", "gross_profit"
    )

    # Attach the promoted product's surrogate key to each campaign.
    camp = dim_campaign.join(
        F.broadcast(dim_product.select("product_key", F.col("product_id").alias("campaign_product_id"))),
        on="campaign_product_id",
        how="inner",
    )

    # ---- campaign-window performance --------------------------------------
    # Measured on the promoted PRODUCT over the campaign's date window rather
    # than on rows tagged with the campaign id. The two agree here, but the
    # product/date definition is the one that stays correct if attribution is
    # ever incomplete — an untagged sale of a promoted product during the
    # promotion is still a promoted sale.
    during = (
        fact.join(F.broadcast(camp), on="product_key", how="inner")
        .where(
            (F.col("transaction_date") >= F.col("start_date"))
            & (F.col("transaction_date") <= F.col("end_date"))
        )
        .groupBy("campaign_key")
        .agg(
            F.sum("net_revenue").alias("campaign_revenue"),
            F.sum("quantity").alias("campaign_units"),
            F.sum("gross_profit").alias("campaign_gross_profit"),
            F.countDistinct("transaction_date").alias("campaign_days_with_sales"),
        )
    )

    # ---- pre-campaign baseline window -------------------------------------
    before = (
        fact.join(F.broadcast(camp), on="product_key", how="inner")
        .where(
            (F.col("transaction_date") < F.col("start_date"))
            & (
                F.col("transaction_date")
                >= F.date_sub(F.col("start_date"), BASELINE_WINDOW_DAYS)
            )
        )
        .groupBy("campaign_key")
        .agg(
            F.sum("net_revenue").alias("baseline_revenue_total"),
            F.sum("quantity").alias("baseline_units_total"),
            F.sum("gross_profit").alias("baseline_gross_profit_total"),
        )
    )

    dur = F.col("duration_days").cast("double")
    base_days = F.lit(float(BASELINE_WINDOW_DAYS))

    mart = (
        camp.join(during, on="campaign_key", how="left")
        .join(before, on="campaign_key", how="left")
        .join(F.broadcast(dim_product), on="product_key", how="left")
        .withColumn("campaign_revenue", F.coalesce("campaign_revenue", F.lit(0)).cast("decimal(16,2)"))
        .withColumn("campaign_units", F.coalesce("campaign_units", F.lit(0)).cast("bigint"))
        .withColumn("campaign_gross_profit",
                    F.coalesce("campaign_gross_profit", F.lit(0)).cast("decimal(16,2)"))
        .withColumn("baseline_revenue_total",
                    F.coalesce("baseline_revenue_total", F.lit(0)).cast("decimal(16,2)"))
        .withColumn("baseline_units_total",
                    F.coalesce("baseline_units_total", F.lit(0)).cast("bigint"))
        .withColumn("baseline_gross_profit_total",
                    F.coalesce("baseline_gross_profit_total", F.lit(0)).cast("decimal(16,2)"))
        .withColumn("baseline_window_days", F.lit(BASELINE_WINDOW_DAYS))
        .withColumn(
            "baseline_daily_revenue",
            (F.col("baseline_revenue_total") / base_days).cast("decimal(16,4)"),
        )
        .withColumn(
            "baseline_daily_units",
            (F.col("baseline_units_total") / base_days).cast("decimal(16,4)"),
        )
        .withColumn(
            "expected_baseline_revenue",
            (F.col("baseline_revenue_total") / base_days * dur).cast("decimal(16,2)"),
        )
        .withColumn(
            "expected_baseline_units",
            (F.col("baseline_units_total") / base_days * dur).cast("decimal(16,2)"),
        )
        .withColumn(
            "expected_baseline_gross_profit",
            (F.col("baseline_gross_profit_total") / base_days * dur).cast("decimal(16,2)"),
        )
        .withColumn(
            "incremental_revenue",
            (F.col("campaign_revenue") - F.col("expected_baseline_revenue")).cast("decimal(16,2)"),
        )
        .withColumn(
            "incremental_units",
            (F.col("campaign_units") - F.col("expected_baseline_units")).cast("decimal(16,2)"),
        )
        .withColumn(
            "incremental_gross_profit",
            (F.col("campaign_gross_profit") - F.col("expected_baseline_gross_profit"))
            .cast("decimal(16,2)"),
        )
        # Guard the divisor: a zero-cost campaign would otherwise yield an
        # infinite ROI that silently poisons every average downstream.
        .withColumn(
            "roi",
            F.when(
                F.col("campaign_cost") > 0,
                (
                    (F.col("incremental_revenue") - F.col("campaign_cost"))
                    / F.col("campaign_cost")
                ),
            ).cast("decimal(12,4)"),
        )
        .withColumn(
            "roi_margin_based",
            F.when(
                F.col("campaign_cost") > 0,
                (
                    (F.col("incremental_gross_profit") - F.col("campaign_cost"))
                    / F.col("campaign_cost")
                ),
            ).cast("decimal(12,4)"),
        )
        .withColumn(
            "uplift_pct",
            F.when(
                F.col("expected_baseline_revenue") > 0,
                (F.col("incremental_revenue") / F.col("expected_baseline_revenue") * 100),
            ).cast("decimal(12,2)"),
        )
        .withColumn("is_profitable", F.col("roi") > 0)
        .select(
            "campaign_key", "campaign_id", "campaign_name",
            "product_key", "product_id", "product_name", "category", "brand",
            "start_date", "end_date", "duration_days", "discount_percentage",
            "campaign_cost",
            "campaign_revenue", "campaign_units", "campaign_gross_profit",
            "baseline_window_days", "baseline_daily_revenue", "baseline_daily_units",
            "expected_baseline_revenue", "expected_baseline_units",
            "incremental_revenue", "incremental_units", "incremental_gross_profit",
            "roi", "roi_margin_based", "uplift_pct", "is_profitable",
        )
    )

    mart.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        MART_CAMPAIGN_ROI
    )

    agg = mart.agg(
        F.count("*").alias("campaigns"),
        F.sum("campaign_cost").alias("total_cost"),
        F.sum("incremental_revenue").alias("total_incremental_revenue"),
        F.sum("campaign_revenue").alias("total_campaign_revenue"),
        F.avg("roi").alias("avg_roi"),
        F.sum(F.when(F.col("is_profitable"), 1).otherwise(0)).alias("profitable_campaigns"),
    ).collect()[0]

    portfolio_roi = None
    if agg["total_cost"] and float(agg["total_cost"]) > 0:
        portfolio_roi = round(
            (float(agg["total_incremental_revenue"]) - float(agg["total_cost"]))
            / float(agg["total_cost"]),
            4,
        )

    return {
        "table": "mart_campaign_roi",
        "campaigns": int(agg["campaigns"]),
        "profitable_campaigns": int(agg["profitable_campaigns"]),
        "total_campaign_cost": round(float(agg["total_cost"] or 0), 2),
        "total_campaign_revenue": round(float(agg["total_campaign_revenue"] or 0), 2),
        "total_incremental_revenue": round(float(agg["total_incremental_revenue"] or 0), 2),
        "average_campaign_roi": round(float(agg["avg_roi"] or 0), 4),
        "portfolio_roi": portfolio_roi,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(build_campaign_roi_mart(), indent=2))
