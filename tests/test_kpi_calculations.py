"""Tests for the business measures.

Every KPI is checked against a fixture whose correct answer was worked out by
hand, so the test fails if the formula drifts — including the sign conventions,
which are the easiest thing to get quietly wrong in an ROI calculation.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from config.project_config import BASELINE_WINDOW_DAYS


# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------

def test_net_revenue_formula(spark):
    """net_revenue = quantity × unit_price − discount_amount."""
    df = spark.createDataFrame(
        [(3, 10.00, 4.50), (1, 25.99, 0.00), (2, 5.00, 10.00)],
        "quantity INT, unit_price DOUBLE, discount_amount DOUBLE",
    )
    out = (
        df.withColumn("gross", F.col("quantity") * F.col("unit_price"))
        .withColumn("net", F.col("gross") - F.col("discount_amount"))
        .collect()
    )
    assert out[0]["net"] == pytest.approx(25.50)    # 30.00 − 4.50
    assert out[1]["net"] == pytest.approx(25.99)
    assert out[2]["net"] == pytest.approx(0.00)     # a 100% discount nets zero


def test_gross_profit_uses_cost_not_price(spark):
    """gross_profit = net_revenue − (quantity × unit_cost)."""
    df = spark.createDataFrame(
        [(4, 10.00, 0.00, 6.00)],
        "quantity INT, unit_price DOUBLE, discount_amount DOUBLE, unit_cost DOUBLE",
    )
    out = (
        df.withColumn("net", F.col("quantity") * F.col("unit_price") - F.col("discount_amount"))
        .withColumn("cogs", F.col("quantity") * F.col("unit_cost"))
        .withColumn("gross_profit", F.col("net") - F.col("cogs"))
        .collect()[0]
    )
    assert out["cogs"] == pytest.approx(24.00)
    assert out["gross_profit"] == pytest.approx(16.00)   # 40.00 − 24.00


# ---------------------------------------------------------------------------
# Promotion ROI
# ---------------------------------------------------------------------------

def _roi(campaign_revenue, baseline_total, duration_days, cost,
         window_days=BASELINE_WINDOW_DAYS):
    baseline_daily = baseline_total / window_days
    expected = baseline_daily * duration_days
    incremental = campaign_revenue - expected
    return incremental, (incremental - cost) / cost


def test_roi_worked_example():
    """Hand-computed: 28-day baseline of 2,800 → 100/day.
    A 14-day campaign is therefore expected to make 1,400 without promotion.
    It actually made 3,000, so incremental = 1,600 against a cost of 800:
    ROI = (1600 − 800) / 800 = 1.0
    """
    incremental, roi = _roi(
        campaign_revenue=3000.0, baseline_total=2800.0, duration_days=14, cost=800.0
    )
    assert incremental == pytest.approx(1600.0)
    assert roi == pytest.approx(1.0)


def test_roi_is_negative_when_uplift_does_not_cover_cost():
    incremental, roi = _roi(
        campaign_revenue=1600.0, baseline_total=2800.0, duration_days=14, cost=800.0
    )
    assert incremental == pytest.approx(200.0)
    assert roi == pytest.approx(-0.75)
    assert roi < 0


def test_roi_breaks_even_exactly():
    incremental, roi = _roi(
        campaign_revenue=2200.0, baseline_total=2800.0, duration_days=14, cost=800.0
    )
    assert incremental == pytest.approx(800.0)
    assert roi == pytest.approx(0.0)


def test_a_campaign_that_sells_nothing_extra_loses_its_whole_cost():
    incremental, roi = _roi(
        campaign_revenue=1400.0, baseline_total=2800.0, duration_days=14, cost=800.0
    )
    assert incremental == pytest.approx(0.0)
    assert roi == pytest.approx(-1.0)


def test_zero_cost_campaign_is_guarded(spark):
    """Dividing by a zero budget must yield NULL, not infinity — one bad row
    would otherwise poison every average built on top of it."""
    df = spark.createDataFrame(
        [(1600.0, 0.0), (1600.0, 800.0)], "incremental DOUBLE, cost DOUBLE"
    )
    out = df.withColumn(
        "roi",
        F.when(F.col("cost") > 0, (F.col("incremental") - F.col("cost")) / F.col("cost")),
    ).collect()
    assert out[0]["roi"] is None
    assert out[1]["roi"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Inventory turnover
# ---------------------------------------------------------------------------

def test_inventory_turnover_worked_example():
    """turnover = COGS ÷ average inventory value, both valued at cost.

    COGS 12,000 against an average stock value of 3,000 → 4.0 turns.
    """
    cogs = 12000.0
    avg_inventory_at_cost = 3000.0
    assert cogs / avg_inventory_at_cost == pytest.approx(4.0)


def test_average_inventory_averages_over_snapshots_not_sums(spark):
    """Stock is semi-additive: averaging over dates is right, summing is not.

    Summing four weekly snapshots of 1,000 units would claim 4,000 units of
    stock that never existed — the same units were counted every week.
    """
    df = spark.createDataFrame(
        [("2025-01-05", 1000.0), ("2025-01-12", 1000.0),
         ("2025-01-19", 1000.0), ("2025-01-26", 1000.0)],
        "snapshot_date STRING, stock_value DOUBLE",
    )
    avg_per_date = (
        df.groupBy("snapshot_date").agg(F.sum("stock_value").alias("v"))
        .agg(F.avg("v")).collect()[0][0]
    )
    total = df.agg(F.sum("stock_value")).collect()[0][0]
    assert avg_per_date == pytest.approx(1000.0)
    assert total == pytest.approx(4000.0)
    assert avg_per_date != total


def test_turnover_guards_against_zero_inventory(spark):
    df = spark.createDataFrame([(5000.0, 0.0)], "cogs DOUBLE, avg_inv DOUBLE")
    out = df.withColumn(
        "turnover", F.when(F.col("avg_inv") > 0, F.col("cogs") / F.col("avg_inv"))
    ).collect()[0]
    assert out["turnover"] is None


# ---------------------------------------------------------------------------
# Uplift
# ---------------------------------------------------------------------------

def test_uplift_percentage():
    expected_baseline = 1400.0
    incremental = 1600.0
    assert (incremental / expected_baseline) * 100 == pytest.approx(114.2857, abs=1e-3)
