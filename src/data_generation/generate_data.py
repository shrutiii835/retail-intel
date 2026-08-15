"""
RetailIntel — synthetic source-system data generator.

Produces five CSV "source system" extracts that behave like a real retail estate:

    products.csv     product master           (reference data)
    stores.csv       store master             (reference data)
    campaigns.csv    promotion master         (reference data)
    sales.csv        POS transaction log      (~132K rows, the main fact feed)
    inventory.csv    weekly stock snapshots

Design rules this generator follows
-----------------------------------
1. Referential integrity is real. Every sales row points at a product the store
   actually carries, at a store that exists, and at a campaign that was live for
   that product on that date — *except* for the small set of orphans we inject
   on purpose in step 9.
2. Behaviour is causal, not random noise. Campaign periods lift the promoted
   product's units. Inventory falls as units sell and is replenished on reorder.
   Weekends sell more than Tuesdays. Without this, ROI and turnover would be
   meaningless numbers and the Power BI report would show nothing.
3. `last_modified_timestamp` is the incremental-load watermark column. It equals
   the transaction timestamp for most rows, and is *later* for the ~1.5% of rows
   that get corrected after the fact (returns, price adjustments). Those late
   corrections are what makes the Delta MERGE in Silver do real work.
4. Defects are injected last, at a small controlled rate, and every injected
   defect is recorded to metrics/injected_defects.json so the pipeline's
   detection can be scored against ground truth.

Everything is seeded — rerunning reproduces byte-identical output.

Usage:  python -m src.data_generation.generate_data
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.project_config import (  # noqa: E402
    DQ_INJECTION_RATES,
    INGESTION_BATCHES,
    INVENTORY_SNAPSHOT_DAY,
    LANDING_DIR,
    METRICS_DIR,
    N_CAMPAIGNS,
    N_PRODUCTS,
    N_SALES_TRANSACTIONS,
    N_STORES,
    PERIOD_END,
    PERIOD_START,
    RANDOM_SEED,
    SAMPLE_DIR,
    SOURCE_DIR,
)

rng = np.random.default_rng(RANDOM_SEED)

TS_FMT = "%Y-%m-%d %H:%M:%S"

# ---------------------------------------------------------------------------
# Reference vocabularies
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Beverages": (["Soft Drinks", "Juices", "Coffee & Tea", "Water"], 1.20, 9.50),
    "Snacks": (["Chips", "Biscuits", "Chocolate", "Nuts"], 0.90, 12.00),
    "Dairy": (["Milk", "Cheese", "Yoghurt", "Butter"], 1.10, 14.00),
    "Personal Care": (["Hair Care", "Skin Care", "Oral Care", "Bath"], 2.40, 28.00),
    "Home Care": (["Detergents", "Cleaners", "Air Care", "Paper Goods"], 1.80, 22.00),
    "Packaged Food": (["Pasta & Rice", "Sauces", "Breakfast", "Canned"], 1.30, 16.00),
    "Frozen": (["Ice Cream", "Frozen Meals", "Frozen Veg"], 2.00, 18.00),
}

BRANDS = [
    "Aurora", "Northwind", "Blue Harbour", "Fairmont", "Kestrel", "Solera",
    "Vantage", "Greenfield", "Marlow", "Pinecrest", "Silverline", "Halcyon",
]

STORE_CITIES = [
    ("Mumbai", "Maharashtra", "West"), ("Pune", "Maharashtra", "West"),
    ("Ahmedabad", "Gujarat", "West"), ("Surat", "Gujarat", "West"),
    ("Delhi", "Delhi", "North"), ("Gurugram", "Haryana", "North"),
    ("Noida", "Uttar Pradesh", "North"), ("Jaipur", "Rajasthan", "North"),
    ("Lucknow", "Uttar Pradesh", "North"), ("Chandigarh", "Punjab", "North"),
    ("Bengaluru", "Karnataka", "South"), ("Mysuru", "Karnataka", "South"),
    ("Chennai", "Tamil Nadu", "South"), ("Coimbatore", "Tamil Nadu", "South"),
    ("Hyderabad", "Telangana", "South"), ("Kochi", "Kerala", "South"),
    ("Visakhapatnam", "Andhra Pradesh", "South"), ("Kolkata", "West Bengal", "East"),
    ("Bhubaneswar", "Odisha", "East"), ("Patna", "Bihar", "East"),
    ("Guwahati", "Assam", "East"), ("Ranchi", "Jharkhand", "East"),
    ("Nagpur", "Maharashtra", "West"), ("Indore", "Madhya Pradesh", "Central"),
    ("Bhopal", "Madhya Pradesh", "Central"), ("Raipur", "Chhattisgarh", "Central"),
    ("Vadodara", "Gujarat", "West"), ("Thane", "Maharashtra", "West"),
]

STORE_FORMATS = ["Hypermarket", "Supermarket", "Express"]
FORMAT_TRAFFIC = {"Hypermarket": 2.4, "Supermarket": 1.0, "Express": 0.45}

CAMPAIGN_THEMES = [
    "New Year Kickoff", "Republic Day Bonanza", "Winter Clearance", "Weekend Blitz",
    "Family Pack Deal", "Buy More Save More", "Spring Refresh", "Payday Special",
    "Festive Combo", "Health Kick", "Summer Cooler", "Pantry Stock-Up",
    "Flash Friday", "Loyalty Boost", "Fresh Start", "Value Days",
]


# ---------------------------------------------------------------------------
# 1. Product master
# ---------------------------------------------------------------------------

def generate_products() -> pd.DataFrame:
    rows = []
    cat_names = list(CATEGORIES)
    # Uneven category sizes — real assortments are not balanced.
    cat_weights = np.array([0.19, 0.18, 0.13, 0.15, 0.12, 0.15, 0.08])
    cat_choice = rng.choice(len(cat_names), size=N_PRODUCTS, p=cat_weights)

    for i in range(N_PRODUCTS):
        category = cat_names[cat_choice[i]]
        subs, lo, hi = CATEGORIES[category]
        sub = subs[rng.integers(0, len(subs))]
        brand = BRANDS[rng.integers(0, len(BRANDS))]

        list_price = float(np.round(rng.uniform(lo, hi), 2))
        # Gross margin 22%–48% → unit_cost. Needed for a real COGS-based turnover.
        margin = rng.uniform(0.22, 0.48)
        unit_cost = float(np.round(list_price * (1 - margin), 2))

        # Popularity is heavy-tailed: a few products drive most volume.
        popularity = float(np.round(rng.pareto(1.7) + 0.35, 4))

        rows.append(
            {
                "product_id": f"P{i + 1:04d}",
                "product_name": f"{brand} {sub} {rng.integers(100, 999)}",
                "category": category,
                "sub_category": sub,
                "brand": brand,
                "unit_cost": unit_cost,
                "list_price": list_price,
                "_popularity": popularity,
            }
        )

    df = pd.DataFrame(rows)
    # Reference data changes rarely; stamp it before the fact window opens.
    df["last_modified_timestamp"] = "2024-12-15 09:00:00"
    return df


# ---------------------------------------------------------------------------
# 2. Store master
# ---------------------------------------------------------------------------

def generate_stores() -> pd.DataFrame:
    rows = []
    for i in range(N_STORES):
        city, state, region = STORE_CITIES[i % len(STORE_CITIES)]
        fmt = STORE_FORMATS[int(rng.choice(3, p=[0.25, 0.5, 0.25]))]
        open_date = datetime(2016, 1, 1) + timedelta(days=int(rng.integers(0, 2900)))
        rows.append(
            {
                "store_id": f"S{i + 1:03d}",
                "store_name": f"RetailIntel {city} {fmt}",
                "city": city,
                "state": state,
                "region": region,
                "store_format": fmt,
                "open_date": open_date.strftime("%Y-%m-%d"),
                # Traffic multiplier drives how much volume the store gets.
                "_traffic": FORMAT_TRAFFIC[fmt] * float(rng.uniform(0.75, 1.3)),
            }
        )
    df = pd.DataFrame(rows)
    df["last_modified_timestamp"] = "2024-12-15 09:00:00"
    return df


# ---------------------------------------------------------------------------
# 3. Store assortment — which store carries which product
# ---------------------------------------------------------------------------

def build_assortment(products: pd.DataFrame, stores: pd.DataFrame) -> dict[str, np.ndarray]:
    """Each store carries a subset of the catalogue (Express carries least).

    Sales are only ever generated for a product the store actually carries, and
    inventory snapshots cover exactly the same set. That is what keeps
    fact→dimension joins honest.
    """
    assortment: dict[str, np.ndarray] = {}
    pids = products["product_id"].to_numpy()
    for _, s in stores.iterrows():
        share = {"Hypermarket": 0.92, "Supermarket": 0.74, "Express": 0.48}[s["store_format"]]
        n = int(len(pids) * share)
        assortment[s["store_id"]] = rng.choice(pids, size=n, replace=False)
    return assortment


# ---------------------------------------------------------------------------
# 4. Campaign master
# ---------------------------------------------------------------------------

def generate_campaigns(products: pd.DataFrame) -> pd.DataFrame:
    """One campaign promotes one product for a contiguous date window.

    Windows are spread across the period so the ROI analysis has campaigns in
    every month, and each promoted product gets at most one campaign so the
    before/after baseline is not polluted by a second promotion.
    """
    period_start = datetime.strptime(PERIOD_START, "%Y-%m-%d")
    period_end = datetime.strptime(PERIOD_END, "%Y-%m-%d")
    total_days = (period_end - period_start).days

    # Promote reasonably popular products — retailers promote what already sells.
    ranked = products.sort_values("_popularity", ascending=False)
    pool = ranked.head(90)["product_id"].to_numpy()
    promoted = rng.choice(pool, size=N_CAMPAIGNS, replace=False)

    # Budget scales with how big the product already is. A retailer does not
    # spend the same on promoting a top-10 line as on a long-tail SKU, and
    # without this the ROI column is just "small products lose money", which
    # tells a reader nothing.
    pop = products.set_index("product_id")["_popularity"]
    median_pop = float(pop.median())

    rows = []
    for i, pid in enumerate(promoted):
        duration = int(rng.choice([7, 10, 14, 14, 21, 28]))
        # Leave room for the 28-day pre-campaign baseline window where possible.
        earliest = 21
        start_offset = int(rng.integers(earliest, max(earliest + 1, total_days - duration)))
        start = period_start + timedelta(days=start_offset)
        end = start + timedelta(days=duration - 1)
        if end > period_end:
            end = period_end
            start = end - timedelta(days=duration - 1)

        discount_pct = float(np.round(rng.choice([5, 10, 10, 15, 15, 20, 25, 30, 35, 40]), 2))
        # Spend scales with reach (duration), depth (discount) and product size.
        # Calibrated so the campaign portfolio lands on a realistic mix of
        # winners and losers rather than all one or all the other.
        size_scale = float(np.clip(np.sqrt(float(pop[pid]) / median_pop), 0.45, 2.8))
        cost = float(np.round(
            (duration * rng.uniform(26, 78) + discount_pct * rng.uniform(13, 39))
            * size_scale,
            2,
        ))

        rows.append(
            {
                "campaign_id": f"C{i + 1:03d}",
                "campaign_name": f"{CAMPAIGN_THEMES[i % len(CAMPAIGN_THEMES)]} {start.strftime('%b')}",
                "product_id": pid,
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "discount_percentage": discount_pct,
                "campaign_cost": cost,
                "last_modified_timestamp": (start - timedelta(days=7)).strftime(TS_FMT),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 5. Sales transactions
# ---------------------------------------------------------------------------

def generate_sales(
    products: pd.DataFrame,
    stores: pd.DataFrame,
    campaigns: pd.DataFrame,
    assortment: dict[str, np.ndarray],
) -> pd.DataFrame:
    """Day-by-day transaction generation with campaign uplift.

    Volume per day = base share shaped by weekday and a mild seasonal curve.
    Within a day, products are drawn by popularity weight; a product that is on
    promotion that day gets an uplift multiplier, which is precisely the signal
    the promotion-ROI measure later recovers.
    """
    period_start = datetime.strptime(PERIOD_START, "%Y-%m-%d")
    period_end = datetime.strptime(PERIOD_END, "%Y-%m-%d")
    days = [period_start + timedelta(days=d) for d in range((period_end - period_start).days + 1)]

    prod_idx = {p: i for i, p in enumerate(products["product_id"])}
    base_pop = products["_popularity"].to_numpy(dtype=float)
    list_price = products["list_price"].to_numpy(dtype=float)

    store_ids = stores["store_id"].to_numpy()
    store_traffic = stores["_traffic"].to_numpy(dtype=float)
    store_p = store_traffic / store_traffic.sum()

    # campaign lookup: date -> {product_id: (campaign_id, discount_pct, uplift)}
    promo_by_day: dict[str, dict[str, tuple[str, float, float]]] = defaultdict(dict)
    for _, c in campaigns.iterrows():
        s = datetime.strptime(c["start_date"], "%Y-%m-%d")
        e = datetime.strptime(c["end_date"], "%Y-%m-%d")
        # Deeper discount → bigger uplift, with diminishing returns.
        uplift = 1.0 + (c["discount_percentage"] / 100.0) * float(rng.uniform(4.5, 8.0))
        d = s
        while d <= e:
            promo_by_day[d.strftime("%Y-%m-%d")][c["product_id"]] = (
                c["campaign_id"], float(c["discount_percentage"]), uplift
            )
            d += timedelta(days=1)

    # Day weighting: weekends busier, payday bump, gentle upward trend.
    weekday_factor = np.array([0.86, 0.82, 0.85, 0.92, 1.15, 1.48, 1.42])  # Mon..Sun
    day_weights = []
    for d in days:
        w = weekday_factor[d.weekday()]
        if d.day in (1, 2, 3, 28, 29, 30, 31):
            w *= 1.12                                   # payday / month-end
        w *= 1.0 + 0.0015 * (d - period_start).days     # slow growth
        day_weights.append(w)
    day_weights = np.array(day_weights)
    day_counts = np.floor(day_weights / day_weights.sum() * N_SALES_TRANSACTIONS).astype(int)
    day_counts[-1] += N_SALES_TRANSACTIONS - day_counts.sum()   # exact target

    # Per-store assortment as index arrays for fast weighted sampling.
    assort_idx = {sid: np.array([prod_idx[p] for p in pids]) for sid, pids in assortment.items()}

    chunks = []
    txn_counter = 0

    for day, n_day in zip(days, day_counts):
        if n_day <= 0:
            continue
        dkey = day.strftime("%Y-%m-%d")
        promos = promo_by_day.get(dkey, {})

        # Which store each of today's transactions happened in.
        s_pick = rng.choice(len(store_ids), size=n_day, p=store_p)

        day_prod = np.empty(n_day, dtype=np.int64)
        for si in np.unique(s_pick):
            mask = s_pick == si
            k = int(mask.sum())
            idxs = assort_idx[store_ids[si]]
            w = base_pop[idxs].copy()
            if promos:
                for pid, (_cid, _disc, uplift) in promos.items():
                    pi = prod_idx[pid]
                    hit = np.nonzero(idxs == pi)[0]
                    if hit.size:
                        w[hit[0]] *= uplift
            w = w / w.sum()
            day_prod[mask] = idxs[rng.choice(len(idxs), size=k, p=w)]

        # Time of day: bimodal (lunch + evening), clipped to opening hours.
        hours = np.where(
            rng.random(n_day) < 0.42,
            rng.normal(12.5, 1.6, n_day),
            rng.normal(18.6, 1.9, n_day),
        )
        hours = np.clip(hours, 8.0, 21.99)
        secs = (hours * 3600).astype(int)

        pid_arr = products["product_id"].to_numpy()[day_prod]
        sid_arr = store_ids[s_pick]

        # Promo rows: bigger baskets. Quantity is a shifted Poisson (never 0).
        promo_pids = set(promos)
        is_promo = np.isin(pid_arr, list(promo_pids)) if promo_pids else np.zeros(n_day, bool)
        lam = np.where(is_promo, 2.1, 0.9)
        quantity = 1 + rng.poisson(lam)
        quantity = np.clip(quantity, 1, 12)

        # Selling price wobbles slightly around list (local pricing).
        unit_price = np.round(list_price[day_prod] * rng.uniform(0.97, 1.03, n_day), 2)

        campaign_id = np.full(n_day, "", dtype=object)
        discount_amount = np.zeros(n_day)
        if promos:
            for pid, (cid, disc, _u) in promos.items():
                m = pid_arr == pid
                if m.any():
                    campaign_id[m] = cid
                    discount_amount[m] = np.round(unit_price[m] * quantity[m] * disc / 100.0, 2)
        # A small amount of non-campaign discounting (clearance, loyalty).
        adhoc = (~is_promo) & (rng.random(n_day) < 0.06)
        discount_amount[adhoc] = np.round(
            unit_price[adhoc] * quantity[adhoc] * rng.uniform(0.02, 0.10, adhoc.sum()), 2
        )

        ts = [
            (day + timedelta(seconds=int(s))).strftime(TS_FMT)
            for s in secs
        ]

        chunk = pd.DataFrame(
            {
                "transaction_id": [
                    f"TXN{txn_counter + i + 1:08d}" for i in range(n_day)
                ],
                "transaction_timestamp": ts,
                "product_id": pid_arr,
                "store_id": sid_arr,
                "quantity": quantity.astype(int),
                "unit_price": unit_price,
                "discount_amount": discount_amount,
                "campaign_id": campaign_id,
            }
        )
        txn_counter += n_day
        chunks.append(chunk)

    sales = pd.concat(chunks, ignore_index=True)

    # ---- last_modified_timestamp: the incremental watermark column ----------
    # Most rows are never touched again → last_modified == transaction time.
    # ~1.5% are corrected days later (returns, re-rings, price fixes). Those
    # arrive in a *later* batch carrying an older transaction date, which is
    # exactly the case a naive "load yesterday's partition" job gets wrong.
    txn_dt = pd.to_datetime(sales["transaction_timestamp"], format=TS_FMT)
    last_mod = txn_dt.copy()

    n = len(sales)
    corrected = rng.random(n) < 0.015
    # Only correct rows early enough that the correction still lands in-period.
    period_end_dt = datetime.strptime(PERIOD_END, "%Y-%m-%d") + timedelta(days=1)
    delay_days = rng.integers(3, 25, n)
    proposed = txn_dt + pd.to_timedelta(delay_days, unit="D")
    corrected &= proposed < period_end_dt
    last_mod[corrected] = proposed[corrected]

    sales["last_modified_timestamp"] = last_mod.dt.strftime(TS_FMT)
    sales["_corrected"] = corrected

    # Corrected rows get an adjusted quantity/discount so the MERGE genuinely
    # changes values (otherwise "upsert" would be indistinguishable from a no-op).
    # The pre-correction values are kept: a real source table is mutable, so the
    # earlier extract would have carried the ORIGINAL version of this row, and
    # the later extract carries the corrected one. write_outputs() emits both,
    # which is what gives the Silver/Gold MERGE something to actually update.
    adj = sales["_corrected"].to_numpy()
    sales["_orig_quantity"] = sales["quantity"].to_numpy().copy()
    sales["_orig_discount_amount"] = sales["discount_amount"].to_numpy().copy()

    q = sales["quantity"].to_numpy().copy()
    q[adj] = np.maximum(1, q[adj] - rng.integers(0, 2, adj.sum()))
    sales["quantity"] = q
    sales["discount_amount"] = np.round(
        np.where(
            adj,
            sales["discount_amount"].to_numpy() * rng.uniform(0.9, 1.1, n),
            sales["discount_amount"].to_numpy(),
        ),
        2,
    )
    return sales


# ---------------------------------------------------------------------------
# 6. Inventory snapshots
# ---------------------------------------------------------------------------

def generate_inventory(
    products: pd.DataFrame,
    stores: pd.DataFrame,
    assortment: dict[str, np.ndarray],
    sales: pd.DataFrame,
) -> pd.DataFrame:
    """Weekly stock snapshots that actually respond to what was sold.

    stock(t) = stock(t-1) - units_sold_in_week + replenishment
    Replenishment fires when cover drops below ~2 weeks of demand, which keeps
    stock positive, keeps average inventory realistic, and makes inventory
    turnover a number with meaning rather than a ratio of two random walks.
    """
    period_start = datetime.strptime(PERIOD_START, "%Y-%m-%d")
    period_end = datetime.strptime(PERIOD_END, "%Y-%m-%d")

    snap_dates = []
    d = period_start
    while d.weekday() != INVENTORY_SNAPSHOT_DAY:
        d += timedelta(days=1)
    while d <= period_end:
        snap_dates.append(d)
        d += timedelta(days=7)

    # Units sold per (store, product, week-ending-date)
    s = sales[["store_id", "product_id", "quantity", "transaction_timestamp"]].copy()
    s["_dt"] = pd.to_datetime(s["transaction_timestamp"], format=TS_FMT)
    bins = [period_start - timedelta(days=7)] + snap_dates + [period_end + timedelta(days=8)]
    s["_bucket"] = pd.cut(s["_dt"], bins=pd.to_datetime(bins), right=True, labels=False)
    weekly = (
        s.groupby(["store_id", "product_id", "_bucket"], observed=True)["quantity"]
        .sum()
        .to_dict()
    )

    rows = []
    inv_counter = 0
    for _, st in stores.iterrows():
        sid = st["store_id"]
        for pid in assortment[sid]:
            # Opening stock ≈ 4 weeks of expected demand, floor of 12 units.
            expected_wk = max(
                1.0,
                float(np.mean([weekly.get((sid, pid, b), 0) for b in range(1, len(snap_dates) + 1)])),
            )
            stock = int(max(12, round(expected_wk * 4 * float(rng.uniform(0.8, 1.4)))))

            for bi, sd in enumerate(snap_dates, start=1):
                sold = int(weekly.get((sid, pid, bi), 0))
                stock = stock - sold
                # Reorder when cover < ~2 weeks.
                if stock < expected_wk * 2:
                    stock += int(round(expected_wk * float(rng.uniform(3.0, 5.0)))) + 8
                stock = max(0, stock)

                inv_counter += 1
                rows.append(
                    {
                        "inventory_id": f"INV{inv_counter:08d}",
                        "snapshot_date": sd.strftime("%Y-%m-%d"),
                        "product_id": pid,
                        "store_id": sid,
                        "stock_quantity": stock,
                        "last_modified_timestamp": (sd + timedelta(hours=23)).strftime(TS_FMT),
                    }
                )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 7. Controlled defect injection
# ---------------------------------------------------------------------------

def inject_defects(sales: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Introduce a small, recorded set of defects into the sales feed.

    Ground truth is returned so DATA_QUALITY.md can compare what we injected
    against what the pipeline caught. Defect types are applied to *disjoint*
    row sets so a single row is never doubly-explained, which keeps the
    scoring unambiguous.
    """
    df = sales.copy()
    n = len(df)
    ledger: dict[str, list[str]] = {}

    # Reserve disjoint row positions for each non-duplicate defect type.
    pool = rng.permutation(n)
    cursor = 0

    def take(rate: float) -> np.ndarray:
        nonlocal cursor
        k = int(round(n * rate))
        sel = pool[cursor: cursor + k]
        cursor += k
        return sel

    # Columns that must tolerate strings once we inject type errors.
    df["quantity"] = df["quantity"].astype(object)
    df["unit_price"] = df["unit_price"].astype(object)
    df["product_id"] = df["product_id"].astype(object)
    df["store_id"] = df["store_id"].astype(object)
    df["campaign_id"] = df["campaign_id"].astype(object)
    df["transaction_timestamp"] = df["transaction_timestamp"].astype(object)

    sel = take(DQ_INJECTION_RATES["null_product_id"])
    df.iloc[sel, df.columns.get_loc("product_id")] = None
    ledger["null_product_id"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["null_store_id"])
    df.iloc[sel, df.columns.get_loc("store_id")] = None
    ledger["null_store_id"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["null_timestamp"])
    df.iloc[sel, df.columns.get_loc("transaction_timestamp")] = None
    ledger["null_timestamp"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["invalid_quantity"])
    bad_q = rng.choice([0, -1, -2, -5], size=len(sel))
    df.iloc[sel, df.columns.get_loc("quantity")] = bad_q
    ledger["invalid_quantity"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["invalid_unit_price"])
    bad_p = [float(x) for x in rng.choice([-1.5, -10.0, -0.99], size=len(sel))]
    df.iloc[sel, df.columns.get_loc("unit_price")] = bad_p
    ledger["invalid_unit_price"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["orphan_product_id"])
    df.iloc[sel, df.columns.get_loc("product_id")] = [
        f"P9{int(x):03d}" for x in rng.integers(100, 999, len(sel))
    ]
    ledger["orphan_product_id"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["orphan_store_id"])
    df.iloc[sel, df.columns.get_loc("store_id")] = [
        f"S9{int(x):02d}" for x in rng.integers(10, 99, len(sel))
    ]
    ledger["orphan_store_id"] = df.iloc[sel]["transaction_id"].tolist()

    sel = take(DQ_INJECTION_RATES["orphan_campaign_id"])
    df.iloc[sel, df.columns.get_loc("campaign_id")] = [
        f"C9{int(x):02d}" for x in rng.integers(10, 99, len(sel))
    ]
    ledger["orphan_campaign_id"] = df.iloc[sel]["transaction_id"].tolist()

    # Type inconsistencies — the source system exported text where a number was
    # expected. Half are recoverable ("3.0", "1,299.00"), half are not ("N/A").
    sel = take(DQ_INJECTION_RATES["type_inconsistency"])
    half = len(sel) // 2
    df.iloc[sel[:half], df.columns.get_loc("quantity")] = [
        f"{int(v)}.0" for v in rng.integers(1, 5, half)
    ]
    df.iloc[sel[half:], df.columns.get_loc("quantity")] = "N/A"
    # Tracked as two separate populations because the correct pipeline response
    # differs: "3.0" is a formatting artefact that must be REPAIRED and loaded,
    # while "N/A" carries no recoverable value and must be REJECTED. Scoring
    # them together would make correct behaviour look like a 50% miss.
    ledger["type_inconsistency_recoverable"] = df.iloc[sel[:half]]["transaction_id"].tolist()
    ledger["type_inconsistency_unrecoverable"] = df.iloc[sel[half:]]["transaction_id"].tolist()

    # Duplicates last: exact re-sends of rows that are otherwise clean, so the
    # duplicate rule is tested independently of the other rules.
    clean_positions = pool[cursor:]
    k_dup = int(round(n * DQ_INJECTION_RATES["duplicate_rows"]))
    dup_positions = rng.choice(clean_positions, size=k_dup, replace=False)
    dup_rows = df.iloc[dup_positions].copy()
    ledger["duplicate_rows"] = dup_rows["transaction_id"].tolist()

    df = pd.concat([df, dup_rows], ignore_index=True)
    # Shuffle so duplicates are not adjacent — a realistic re-send arrives later.
    df = df.sample(frac=1.0, random_state=RANDOM_SEED).reset_index(drop=True)

    summary = {
        "clean_rows_generated": int(n),
        "duplicate_rows_appended": int(k_dup),
        "total_rows_written": int(len(df)),
        "defect_counts": {k: len(v) for k, v in ledger.items()},
        "defect_rate_pct": round(
            100.0 * sum(len(v) for v in ledger.values()) / len(df), 4
        ),
        "transaction_ids": ledger,
    }
    return df, summary


# ---------------------------------------------------------------------------
# 8. Write source + landing batches
# ---------------------------------------------------------------------------

def write_outputs(
    products: pd.DataFrame,
    stores: pd.DataFrame,
    campaigns: pd.DataFrame,
    sales: pd.DataFrame,
    inventory: pd.DataFrame,
) -> dict:
    for d in (SOURCE_DIR, LANDING_DIR, SAMPLE_DIR, METRICS_DIR):
        Path(d).mkdir(parents=True, exist_ok=True)

    internal = ("_corrected", "_orig_quantity", "_orig_discount_amount")
    pub_products = products.drop(columns=["_popularity"])
    pub_stores = stores.drop(columns=["_traffic"])
    pub_sales = sales.drop(columns=[c for c in internal if c in sales.columns])

    pub_products.to_csv(SOURCE_DIR / "products.csv", index=False)
    pub_stores.to_csv(SOURCE_DIR / "stores.csv", index=False)
    campaigns.to_csv(SOURCE_DIR / "campaigns.csv", index=False)
    pub_sales.to_csv(SOURCE_DIR / "sales.csv", index=False)
    inventory.to_csv(SOURCE_DIR / "inventory.csv", index=False)

    # ---- landing batches --------------------------------------------------
    # These are what ADF's Copy activity picks up. Each batch contains rows
    # whose last_modified_timestamp falls in that batch's window — the same
    # predicate the watermark logic applies, materialised as files so the
    # pipeline can run without a live source database.
    #
    # A source table is mutable, so a transaction that is later corrected was
    # ALSO present, in its original form, in the earlier extract. We reconstruct
    # that history here: every corrected transaction contributes two versions,
    # one stamped at the sale time and one at the correction time. Those two
    # versions land in different batches, which is precisely the case that an
    # append-only load gets wrong and a MERGE gets right.
    if "_corrected" in sales.columns:
        corrected = sales[sales["_corrected"]].copy()
        prior = corrected.drop(columns=[c for c in internal if c in corrected.columns]).copy()
        prior["quantity"] = corrected["_orig_quantity"].to_numpy()
        prior["discount_amount"] = corrected["_orig_discount_amount"].to_numpy()
        # The original version's last_modified is the moment of sale.
        prior["last_modified_timestamp"] = prior["transaction_timestamp"]
        # A row whose timestamp was nulled by defect injection has no usable
        # version stamp, so it only ever appears once.
        prior = prior[prior["last_modified_timestamp"].notna()]
        wire_sales = pd.concat([pub_sales, prior], ignore_index=True)
    else:
        prior = pd.DataFrame()
        wire_sales = pub_sales

    lm = pd.to_datetime(
        wire_sales["last_modified_timestamp"], format=TS_FMT, errors="coerce"
    )
    inv_lm = pd.to_datetime(inventory["last_modified_timestamp"], format=TS_FMT)

    batch_info = []
    lower = pd.Timestamp("1900-01-01")
    for b in INGESTION_BATCHES:
        upper = pd.Timestamp(b.modified_through)
        bdir = Path(LANDING_DIR) / b.batch_id
        (bdir / "sales").mkdir(parents=True, exist_ok=True)
        (bdir / "inventory").mkdir(parents=True, exist_ok=True)

        s_slice = wire_sales[(lm > lower) & (lm <= upper)]
        i_slice = inventory[(inv_lm > lower) & (inv_lm <= upper)]
        s_slice.to_csv(bdir / "sales" / "sales.csv", index=False)
        i_slice.to_csv(bdir / "inventory" / "inventory.csv", index=False)

        # Reference data ships with every batch — small, and a full refresh of
        # dimensions is cheaper than tracking their deltas at this size.
        (bdir / "campaigns").mkdir(parents=True, exist_ok=True)
        (bdir / "products").mkdir(parents=True, exist_ok=True)
        (bdir / "stores").mkdir(parents=True, exist_ok=True)
        campaigns.to_csv(bdir / "campaigns" / "campaigns.csv", index=False)
        pub_products.to_csv(bdir / "products" / "products.csv", index=False)
        pub_stores.to_csv(bdir / "stores" / "stores.csv", index=False)

        batch_info.append(
            {
                "batch_id": b.batch_id,
                "label": b.label,
                "window_upper_bound": b.modified_through,
                "sales_rows": int(len(s_slice)),
                "inventory_rows": int(len(i_slice)),
                "description": b.description,
            }
        )
        lower = upper

    # Batch 3 deliberately re-sends part of batch 2 unchanged, so that running
    # the pipeline over overlapping data proves idempotency rather than
    # silently double-counting.
    b2 = Path(LANDING_DIR) / INGESTION_BATCHES[1].batch_id / "sales" / "sales.csv"
    b3 = Path(LANDING_DIR) / INGESTION_BATCHES[2].batch_id / "sales" / "sales.csv"
    b2_df = pd.read_csv(b2, dtype=str)
    b3_df = pd.read_csv(b3, dtype=str)
    resend = b2_df.tail(max(1, int(len(b2_df) * 0.10)))
    pd.concat([b3_df, resend], ignore_index=True).to_csv(b3, index=False)
    batch_info[2]["sales_rows"] = int(len(b3_df) + len(resend))
    batch_info[2]["deliberate_resend_rows"] = int(len(resend))

    # ---- committed samples (small enough for git) -------------------------
    pub_sales.head(1000).to_csv(SAMPLE_DIR / "sales_sample.csv", index=False)
    inventory.head(500).to_csv(SAMPLE_DIR / "inventory_sample.csv", index=False)
    campaigns.to_csv(SAMPLE_DIR / "campaigns_sample.csv", index=False)
    pub_products.head(50).to_csv(SAMPLE_DIR / "products_sample.csv", index=False)
    pub_stores.to_csv(SAMPLE_DIR / "stores_sample.csv", index=False)

    return {
        "batches": batch_info,
        "prior_versions_emitted": int(len(prior)),
        "total_wire_rows": int(len(wire_sales)),
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    print("RetailIntel — generating synthetic source data")
    print(f"  seed={RANDOM_SEED}  period={PERIOD_START}..{PERIOD_END}")

    products = generate_products()
    stores = generate_stores()
    assortment = build_assortment(products, stores)
    campaigns = generate_campaigns(products)
    print(f"  products={len(products)}  stores={len(stores)}  campaigns={len(campaigns)}")

    sales = generate_sales(products, stores, campaigns, assortment)
    print(f"  clean sales transactions={len(sales):,}")

    inventory = generate_inventory(products, stores, assortment, sales)
    print(f"  inventory snapshots={len(inventory):,}")

    sales_dirty, defect_summary = inject_defects(sales)
    print(
        f"  sales rows written={len(sales_dirty):,} "
        f"(defect rate {defect_summary['defect_rate_pct']}%)"
    )

    out = write_outputs(products, stores, campaigns, sales_dirty, inventory)

    corrected = int(sales["_corrected"].sum())
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime(TS_FMT),
        "seed": RANDOM_SEED,
        "period": {"start": PERIOD_START, "end": PERIOD_END},
        "row_counts": {
            "products": int(len(products)),
            "stores": int(len(stores)),
            "campaigns": int(len(campaigns)),
            "sales_clean": int(len(sales)),
            "sales_written_with_defects": int(len(sales_dirty)),
            "inventory": int(len(inventory)),
        },
        "late_corrections": corrected,
        "late_correction_pct": round(100.0 * corrected / len(sales), 3),
        "defects": defect_summary,
        "ingestion": out,
    }
    Path(METRICS_DIR).mkdir(parents=True, exist_ok=True)
    with open(Path(METRICS_DIR) / "data_generation_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n  batches:")
    for b in out["batches"]:
        print(
            f"    {b['batch_id']:<22} sales={b['sales_rows']:>7,}  "
            f"inventory={b['inventory_rows']:>6,}"
        )
    print(f"\n  wrote metrics/data_generation_summary.json")
    print("  done.")


if __name__ == "__main__":
    main()
