"""Tests for the generated source data.

These guard the *premises* the rest of the project rests on. If sales reference
products that do not exist, or campaign windows are backwards, then every
downstream number is meaningless and the failure would surface as a confusing
data-quality result rather than an obvious generator bug.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from config.project_config import (
    DQ_INJECTION_RATES,
    METRICS_DIR,
    N_CAMPAIGNS,
    N_PRODUCTS,
    N_SALES_TRANSACTIONS,
    N_STORES,
    PERIOD_END,
    PERIOD_START,
)


def test_volume_meets_resume_claim(generated):
    """The resume says 120K+ transactions; the data must actually contain them."""
    n = len(generated["sales"])
    assert n >= 120_000, f"only {n:,} sales rows — below the 120K+ claim"
    assert n <= 160_000, f"{n:,} rows is well beyond the intended mini-project size"


def test_master_data_sizes(generated):
    assert len(generated["products"]) == N_PRODUCTS
    assert len(generated["stores"]) == N_STORES
    assert len(generated["campaigns"]) == N_CAMPAIGNS


def test_transaction_ids_are_unique_apart_from_injected_duplicates(generated):
    sales = generated["sales"]
    dup_count = len(sales) - sales["transaction_id"].nunique()
    expected = int(round(N_SALES_TRANSACTIONS * DQ_INJECTION_RATES["duplicate_rows"]))
    # Exact-resend duplicates are injected deliberately; nothing else should
    # produce a repeated transaction id.
    assert dup_count == pytest.approx(expected, abs=5), (
        f"{dup_count} duplicate ids, expected ~{expected} injected re-sends"
    )


def test_referential_integrity_holds_except_for_injected_orphans(generated):
    """Every reference must resolve except the orphans we injected on purpose."""
    sales = generated["sales"]
    valid_products = set(generated["products"]["product_id"])
    valid_stores = set(generated["stores"]["store_id"])
    valid_campaigns = set(generated["campaigns"]["campaign_id"])

    truth = json.loads(
        (Path(METRICS_DIR) / "data_generation_summary.json").read_text()
    )["defects"]["transaction_ids"]

    def orphan_ids(col: str, valid: set[str]) -> set[str]:
        present = sales[sales[col].notna()]
        bad = present[~present[col].isin(valid)]
        return set(bad["transaction_id"])

    # Injected orphans are known by transaction id, so any orphan NOT in that
    # ledger is an accidental broken reference — a real generator bug.
    unexpected_products = orphan_ids("product_id", valid_products) - set(
        truth["orphan_product_id"]
    )
    unexpected_stores = orphan_ids("store_id", valid_stores) - set(truth["orphan_store_id"])
    campaign_present = sales[sales["campaign_id"].notna()]
    campaign_present = campaign_present[campaign_present["campaign_id"] != ""]
    unexpected_campaigns = set(
        campaign_present[~campaign_present["campaign_id"].isin(valid_campaigns)][
            "transaction_id"
        ]
    ) - set(truth["orphan_campaign_id"])

    assert not unexpected_products, f"{len(unexpected_products)} unplanned product orphans"
    assert not unexpected_stores, f"{len(unexpected_stores)} unplanned store orphans"
    assert not unexpected_campaigns, f"{len(unexpected_campaigns)} unplanned campaign orphans"


def test_campaign_windows_are_logical(generated):
    c = generated["campaigns"].copy()
    c["start"] = pd.to_datetime(c["start_date"])
    c["end"] = pd.to_datetime(c["end_date"])
    assert (c["end"] >= c["start"]).all(), "a campaign ends before it starts"
    assert (c["start"] >= pd.Timestamp(PERIOD_START)).all()
    assert (c["end"] <= pd.Timestamp(PERIOD_END)).all()

    disc = c["discount_percentage"].astype(float)
    assert disc.between(0, 100).all(), "discount percentage outside 0–100"
    assert (c["campaign_cost"].astype(float) >= 0).all()


def test_campaign_products_exist(generated):
    valid = set(generated["products"]["product_id"])
    assert set(generated["campaigns"]["product_id"]).issubset(valid)


def test_prices_and_costs_are_sane(generated):
    p = generated["products"]
    cost = p["unit_cost"].astype(float)
    price = p["list_price"].astype(float)
    assert (cost >= 0).all()
    assert (price >= cost).all(), "a product sells below cost in the master data"


def test_inventory_grain_is_unique(generated):
    inv = generated["inventory"]
    grain = inv.groupby(["snapshot_date", "product_id", "store_id"]).size()
    assert (grain == 1).all(), "duplicate inventory snapshot for a product/store/date"


def test_inventory_references_are_valid(generated):
    inv = generated["inventory"]
    assert set(inv["product_id"]).issubset(set(generated["products"]["product_id"]))
    assert set(inv["store_id"]).issubset(set(generated["stores"]["store_id"]))
    assert (inv["stock_quantity"].astype(int) >= 0).all()


def test_transactions_fall_inside_the_reporting_period(generated):
    ts = pd.to_datetime(generated["sales"]["transaction_timestamp"], errors="coerce")
    present = ts.dropna()
    assert present.min() >= pd.Timestamp(PERIOD_START)
    assert present.max() < pd.Timestamp(PERIOD_END) + pd.Timedelta(days=1)


def test_defect_rate_stays_small(generated):
    """Defects must prove the pipeline works, not overwhelm the dataset."""
    summary = json.loads(
        (Path(METRICS_DIR) / "data_generation_summary.json").read_text()
    )
    assert summary["defects"]["defect_rate_pct"] < 5.0


def test_watermark_column_is_never_before_the_transaction(generated):
    """last_modified must never precede the sale, or the watermark is nonsense."""
    s = generated["sales"]
    ts = pd.to_datetime(s["transaction_timestamp"], errors="coerce")
    lm = pd.to_datetime(s["last_modified_timestamp"], errors="coerce")
    both = ts.notna() & lm.notna()
    assert (lm[both] >= ts[both]).all()


def test_campaign_periods_show_uplift(generated):
    """A promoted product must actually sell more while promoted.

    Without this the ROI measure would be computing a difference that the data
    never contained, and every campaign would look neutral.
    """
    sales = generated["sales"].copy()
    sales["ts"] = pd.to_datetime(sales["transaction_timestamp"], errors="coerce")
    sales["qty"] = pd.to_numeric(sales["quantity"], errors="coerce")
    sales = sales.dropna(subset=["ts", "qty", "product_id"])

    camps = generated["campaigns"]
    uplifts = []
    for _, c in camps.iterrows():
        start, end = pd.Timestamp(c["start_date"]), pd.Timestamp(c["end_date"])
        dur = (end - start).days + 1
        prod = sales[sales["product_id"] == c["product_id"]]
        during = prod[(prod["ts"] >= start) & (prod["ts"] <= end + pd.Timedelta(days=1))]
        pre_start = start - pd.Timedelta(days=28)
        before = prod[(prod["ts"] >= pre_start) & (prod["ts"] < start)]
        if len(before) == 0 or dur == 0:
            continue
        uplifts.append((during["qty"].sum() / dur) / max(before["qty"].sum() / 28, 0.01))

    assert uplifts, "no campaign had a usable baseline window"
    median_uplift = pd.Series(uplifts).median()
    assert median_uplift > 1.2, (
        f"median campaign uplift {median_uplift:.2f}× — promotions are not "
        "visibly lifting volume, so ROI would be meaningless"
    )
