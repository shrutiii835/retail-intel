"""Post-load consistency assertions and the data-quality scorecard.

This module produces the evidence behind the ">99% data consistency across
curated datasets" claim. It measures three different things, and keeping them
apart is the whole point — conflating them is how that kind of claim becomes
dishonest.

  1. SOURCE REJECTION RATE — what fraction of raw rows the pipeline refused.
     This is NOT the consistency figure. A high rejection rate means the source
     is dirty and the pipeline is doing its job. Reported separately.

  2. CURATED CONSISTENCY — of the rows that DID reach Gold, what fraction is
     fully self-consistent: unique on its primary key, every foreign key
     resolvable to a real dimension member (not the Unknown placeholder), and
     every measure within its declared domain. This is the >99% claim.

  3. RECONCILIATION — does Gold still agree with Silver on row count and total
     revenue? A pipeline can be internally consistent and still have lost rows
     in a join. Variance must be zero.

Detection recall is also scored: the generator records exactly which
transaction_ids it corrupted, so we can check what the rules actually caught
rather than assuming.

    python -m src.quality.consistency_report
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from config.project_config import (  # noqa: E402
    DQ_RESULTS_TABLE,
    GOLD_TABLES,
    METRICS_DIR,
    QUARANTINE_TABLES,
    RUN_LOG_TABLE,
    SILVER_TABLES,
)
from src.common.spark_session import get_spark, stop_spark  # noqa: E402

UNKNOWN_KEY = -1
NO_CAMPAIGN_KEY = 0


def _load(path: str):
    return get_spark().read.format("delta").load(path)


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def assert_fact_sales() -> tuple[list[dict], int, int]:
    """Row-level assertions on FactSales. Returns (results, total, defect_rows)."""
    spark = get_spark()
    fact = _load(GOLD_TABLES["fact_sales"])
    total = fact.count()

    dim_p = _load(GOLD_TABLES["dim_product"]).select(F.col("product_key").alias("_pk"))
    dim_s = _load(GOLD_TABLES["dim_store"]).select(F.col("store_key").alias("_sk"))
    dim_c = _load(GOLD_TABLES["dim_campaign"]).select(F.col("campaign_key").alias("_ck"))
    dim_d = _load(GOLD_TABLES["dim_date"]).select(F.col("date_key").alias("_dk"))

    j = (
        fact.join(F.broadcast(dim_p), fact.product_key == F.col("_pk"), "left")
        .join(F.broadcast(dim_s), fact.store_key == F.col("_sk"), "left")
        .join(F.broadcast(dim_c), fact.campaign_key == F.col("_ck"), "left")
        .join(F.broadcast(dim_d), fact.date_key == F.col("_dk"), "left")
    )

    # Each check is a boolean column so a single pass gives every count AND the
    # number of DISTINCT rows failing at least one check (a row failing three
    # checks is still one bad row — summing per-check failures would triple it).
    checks = {
        "FS_A01_no_null_foreign_keys":
            "product_key IS NULL OR store_key IS NULL OR campaign_key IS NULL "
            "OR date_key IS NULL",
        "FS_A02_product_key_resolves": "_pk IS NULL",
        "FS_A03_store_key_resolves": "_sk IS NULL",
        "FS_A04_campaign_key_resolves": "_ck IS NULL",
        "FS_A05_date_key_resolves": "_dk IS NULL",
        # Pointing at an Unknown member is not a broken join — the row is in the
        # table and its revenue counts — but its attribution is degraded, so it
        # is counted as a consistency defect rather than quietly ignored.
        "FS_A06_no_unknown_product": f"product_key = {UNKNOWN_KEY}",
        "FS_A07_no_unknown_store": f"store_key = {UNKNOWN_KEY}",
        "FS_A08_no_unknown_campaign": f"campaign_key = {UNKNOWN_KEY}",
        "FS_A09_quantity_positive": "quantity IS NULL OR quantity <= 0",
        "FS_A10_unit_price_non_negative": "unit_price IS NULL OR unit_price < 0",
        "FS_A11_discount_non_negative": "discount_amount IS NULL OR discount_amount < 0",
        "FS_A12_revenue_not_null": "net_revenue IS NULL",
        "FS_A13_revenue_arithmetic":
            "abs(net_revenue - (gross_amount - discount_amount)) > 0.01",
        "FS_A14_date_not_null": "transaction_date IS NULL",
    }

    tagged = j
    for name, bad_expr in checks.items():
        tagged = tagged.withColumn(f"_bad_{name}", F.expr(bad_expr))
    any_bad = " OR ".join(f"_bad_{n}" for n in checks)
    tagged = tagged.withColumn("_any_defect", F.expr(any_bad)).cache()

    agg = tagged.agg(
        *[F.sum(F.when(F.col(f"_bad_{n}"), 1).otherwise(0)).alias(n) for n in checks],
        F.sum(F.when(F.col("_any_defect"), 1).otherwise(0)).alias("_defect_rows"),
    ).collect()[0]

    results = [
        {
            "assertion": n,
            "rows_evaluated": total,
            "rows_failed": int(agg[n] or 0),
            "passed": int(agg[n] or 0) == 0,
        }
        for n in checks
    ]

    # Primary-key uniqueness — the single most important proof that dedup and
    # MERGE worked. If this fails, every revenue number is overstated.
    distinct_ids = fact.select("transaction_id").distinct().count()
    dup_rows = total - distinct_ids
    results.append(
        {
            "assertion": "FS_A15_transaction_id_unique",
            "rows_evaluated": total,
            "rows_failed": dup_rows,
            "passed": dup_rows == 0,
        }
    )

    defect_rows = int(agg["_defect_rows"] or 0) + dup_rows
    tagged.unpersist()
    return results, total, defect_rows


def assert_fact_inventory() -> tuple[list[dict], int, int]:
    fact = _load(GOLD_TABLES["fact_inventory_snapshot"])
    total = fact.count()

    checks = {
        "FI_A01_no_null_foreign_keys":
            "product_key IS NULL OR store_key IS NULL OR date_key IS NULL",
        "FI_A02_no_unknown_product": f"product_key = {UNKNOWN_KEY}",
        "FI_A03_no_unknown_store": f"store_key = {UNKNOWN_KEY}",
        "FI_A04_stock_non_negative": "stock_quantity IS NULL OR stock_quantity < 0",
        "FI_A05_stock_value_non_negative":
            "stock_value_at_cost IS NULL OR stock_value_at_cost < 0",
    }
    tagged = fact
    for name, bad in checks.items():
        tagged = tagged.withColumn(f"_bad_{name}", F.expr(bad))
    tagged = tagged.withColumn(
        "_any_defect", F.expr(" OR ".join(f"_bad_{n}" for n in checks))
    ).cache()

    agg = tagged.agg(
        *[F.sum(F.when(F.col(f"_bad_{n}"), 1).otherwise(0)).alias(n) for n in checks],
        F.sum(F.when(F.col("_any_defect"), 1).otherwise(0)).alias("_defect_rows"),
    ).collect()[0]

    results = [
        {"assertion": n, "rows_evaluated": total, "rows_failed": int(agg[n] or 0),
         "passed": int(agg[n] or 0) == 0}
        for n in checks
    ]

    grain = fact.select("date_key", "product_key", "store_key").distinct().count()
    dup = total - grain
    results.append({"assertion": "FI_A06_grain_unique", "rows_evaluated": total,
                    "rows_failed": dup, "passed": dup == 0})

    defect_rows = int(agg["_defect_rows"] or 0) + dup
    tagged.unpersist()
    return results, total, defect_rows


def reconcile_silver_to_gold() -> list[dict]:
    """Gold must still agree with Silver on both count and money."""
    s = _load(SILVER_TABLES["sales"])
    g = _load(GOLD_TABLES["fact_sales"])
    s_rows, g_rows = s.count(), g.count()
    s_rev = float(s.agg(F.sum("net_revenue")).collect()[0][0] or 0)
    g_rev = float(g.agg(F.sum("net_revenue")).collect()[0][0] or 0)

    si = _load(SILVER_TABLES["inventory"])
    gi = _load(GOLD_TABLES["fact_inventory_snapshot"])
    si_rows, gi_rows = si.count(), gi.count()

    return [
        {"check": "sales_row_count", "silver": s_rows, "gold": g_rows,
         "variance": g_rows - s_rows, "passed": s_rows == g_rows},
        {"check": "sales_net_revenue", "silver": round(s_rev, 2), "gold": round(g_rev, 2),
         "variance": round(g_rev - s_rev, 2), "passed": abs(g_rev - s_rev) < 0.01},
        {"check": "inventory_row_count", "silver": si_rows, "gold": gi_rows,
         "variance": gi_rows - si_rows, "passed": si_rows == gi_rows},
    ]


# ---------------------------------------------------------------------------
# Detection recall vs the generator's ground truth
# ---------------------------------------------------------------------------

RULE_FOR_DEFECT = {
    "null_product_id": "SAL_002",
    "null_store_id": "SAL_003",
    "null_timestamp": "SAL_004",
    "invalid_quantity": "SAL_006",
    "invalid_unit_price": "SAL_008",
    "orphan_product_id": "SAL_012",
    "orphan_store_id": "SAL_013",
    "type_inconsistency_unrecoverable": "SAL_005",
}


def detection_recall() -> list[dict]:
    """Compare injected defects against what the rules actually flagged.

    Recall is measured on the quarantine table, which keeps the failed rule ids
    per row, so this is a direct check rather than an inference from counts.
    """
    spark = get_spark()
    gen_path = Path(METRICS_DIR) / "data_generation_summary.json"
    if not gen_path.exists():
        return []
    truth = json.loads(gen_path.read_text())["defects"]["transaction_ids"]

    from delta.tables import DeltaTable
    qpath = QUARANTINE_TABLES["sales"]
    if not DeltaTable.isDeltaTable(spark, qpath):
        return []
    q = _load(qpath).select("transaction_id", "_dq_failed_rules").cache()

    out = []
    for defect, rule_id in RULE_FOR_DEFECT.items():
        ids = truth.get(defect, [])
        if not ids:
            continue
        injected = len(ids)
        ids_df = spark.createDataFrame([(i,) for i in ids], "transaction_id STRING")
        caught = (
            q.join(ids_df, on="transaction_id", how="inner")
            .where(F.array_contains(F.col("_dq_failed_rules"), rule_id))
            .select("transaction_id")
            .distinct()
            .count()
        )
        out.append({
            "defect_type": defect,
            "expected_rule": rule_id,
            "injected": injected,
            "detected": caught,
            "recall_pct": round(100.0 * caught / injected, 2) if injected else None,
        })

    # Recoverable type defects must NOT be quarantined — the correct outcome is
    # that they reached Gold with the value parsed back to a valid quantity.
    rec_ids = truth.get("type_inconsistency_recoverable", [])
    if rec_ids:
        ids_df = spark.createDataFrame([(i,) for i in rec_ids], "transaction_id STRING")
        fact = _load(GOLD_TABLES["fact_sales"]).select("transaction_id", "quantity")
        repaired = (
            fact.join(ids_df, on="transaction_id", how="inner")
            .where("quantity IS NOT NULL AND quantity > 0")
            .count()
        )
        out.append({
            "defect_type": "type_inconsistency_recoverable",
            "expected_rule": "SAL_005 (repair via try_cast → whole number)",
            "injected": len(rec_ids),
            "detected": repaired,
            "recall_pct": round(100.0 * repaired / len(rec_ids), 2),
            "note": "Correct outcome is REPAIR, not rejection: '3.0' is a "
                    "formatting artefact carrying a valid quantity. 'detected' "
                    "counts rows that reached Gold with a valid quantity.",
        })

    # Orphan campaigns are repaired, not quarantined, so they are scored on the
    # fact table instead: every one should have landed on the Unknown member.
    orphan_ids = truth.get("orphan_campaign_id", [])
    if orphan_ids:
        ids_df = spark.createDataFrame(
            [(i,) for i in orphan_ids], "transaction_id STRING"
        )
        fact = _load(GOLD_TABLES["fact_sales"]).select(
            "transaction_id", "campaign_key", "campaign_resolved"
        )
        routed = (
            fact.join(ids_df, on="transaction_id", how="inner")
            .where(
                (F.col("campaign_key") == UNKNOWN_KEY) | (F.col("campaign_resolved") == False)  # noqa: E712
            )
            .count()
        )
        present = fact.join(ids_df, on="transaction_id", how="inner").count()
        out.append({
            "defect_type": "orphan_campaign_id",
            "expected_rule": "SAL_014 (repair → Unknown member)",
            "injected": len(orphan_ids),
            "detected": routed,
            "reached_gold": present,
            "recall_pct": round(100.0 * routed / len(orphan_ids), 2),
            "note": "Repaired rather than rejected: the sale is real, only its "
                    "campaign attribution is unknown.",
        })

    q.unpersist()
    return out


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report() -> dict:
    spark = get_spark()

    runs = _load(RUN_LOG_TABLE)
    ingest = runs.where("layer = 'bronze' AND table_name = 'sales'").agg(
        F.sum("rows_written").alias("ingested")
    ).collect()[0]["ingested"] or 0
    silver_stats = runs.where("layer = 'silver' AND table_name = 'sales'").agg(
        F.sum("rows_read").alias("read"),
        F.sum("rows_rejected").alias("rejected"),
        F.sum("rows_duplicate").alias("duplicates"),
        F.sum("rows_inserted").alias("inserted"),
        F.sum("rows_updated").alias("updated"),
    ).collect()[0]

    fs_results, fs_total, fs_defects = assert_fact_sales()
    fi_results, fi_total, fi_defects = assert_fact_inventory()
    recon = reconcile_silver_to_gold()
    recall = detection_recall()

    curated_rows = fs_total + fi_total
    defect_rows = fs_defects + fi_defects
    consistency_pct = (
        100.0 * (curated_rows - defect_rows) / curated_rows if curated_rows else 0.0
    )

    read = int(silver_stats["read"] or 0)
    rejected = int(silver_stats["rejected"] or 0)

    dq = _load(DQ_RESULTS_TABLE).where("table_name = 'sales'").groupBy(
        "rule_id", "rule_description", "severity"
    ).agg(
        F.sum("rows_evaluated").alias("rows_evaluated"),
        F.sum("rows_failed").alias("rows_failed"),
    ).orderBy("rule_id")
    dq_rows = [r.asDict() for r in dq.collect()]

    report = {
        "curated_consistency": {
            "definition": (
                "Share of rows in the curated (Gold) layer that pass every "
                "post-load assertion: unique primary key, all foreign keys "
                "resolvable to a real (non-Unknown) dimension member, and all "
                "measures inside their declared domain."
            ),
            "curated_rows_evaluated": curated_rows,
            "defect_rows": defect_rows,
            "consistency_pct": round(consistency_pct, 4),
            "meets_99_pct_claim": consistency_pct > 99.0,
        },
        "source_data_quality": {
            "note": (
                "Reported separately from consistency. Rejected rows are the "
                "pipeline working correctly, not a defect in curated data."
            ),
            "bronze_sales_rows_ingested": int(ingest),
            "silver_sales_rows_read": read,
            "rows_rejected_to_quarantine": rejected,
            "rejection_rate_pct": round(100.0 * rejected / read, 4) if read else 0.0,
            "duplicate_rows_collapsed": int(silver_stats["duplicates"] or 0),
            "rows_inserted": int(silver_stats["inserted"] or 0),
            "rows_updated_by_merge": int(silver_stats["updated"] or 0),
        },
        "fact_sales_assertions": fs_results,
        "fact_inventory_assertions": fi_results,
        "silver_to_gold_reconciliation": recon,
        "detection_recall_vs_injected_ground_truth": recall,
        "dq_rule_results_sales": dq_rows,
        "row_counts": {
            "fact_sales": fs_total,
            "fact_inventory_snapshot": fi_total,
        },
    }

    all_assertions = fs_results + fi_results
    report["summary"] = {
        "assertions_run": len(all_assertions) + len(recon),
        "assertions_passed": sum(1 for a in all_assertions if a["passed"])
        + sum(1 for r in recon if r["passed"]),
        "assertions_failed": sum(1 for a in all_assertions if not a["passed"])
        + sum(1 for r in recon if not r["passed"]),
    }
    return report


def main() -> None:
    report = build_report()
    Path(METRICS_DIR).mkdir(parents=True, exist_ok=True)
    out = Path(METRICS_DIR) / "data_quality_report.json"
    out.write_text(json.dumps(report, indent=2, default=str))

    c = report["curated_consistency"]
    s = report["source_data_quality"]
    print("\n=== RetailIntel consistency report ===")
    print(f"  curated rows evaluated : {c['curated_rows_evaluated']:,}")
    print(f"  defect rows            : {c['defect_rows']:,}")
    print(f"  CONSISTENCY            : {c['consistency_pct']}%  "
          f"(>99% claim: {'MET' if c['meets_99_pct_claim'] else 'NOT MET'})")
    print(f"  assertions             : {report['summary']['assertions_passed']}"
          f"/{report['summary']['assertions_run']} passed")
    print(f"\n  source rows read       : {s['silver_sales_rows_read']:,}")
    print(f"  rejected to quarantine : {s['rows_rejected_to_quarantine']:,} "
          f"({s['rejection_rate_pct']}%)")
    print(f"  duplicates collapsed   : {s['duplicate_rows_collapsed']:,}")
    print(f"  updated by MERGE       : {s['rows_updated_by_merge']:,}")

    failed = [a for a in report["fact_sales_assertions"] + report["fact_inventory_assertions"]
              if not a["passed"]]
    if failed:
        print("\n  FAILING ASSERTIONS:")
        for a in failed:
            print(f"    {a['assertion']}: {a['rows_failed']:,} rows")
    print("\n  recall vs injected ground truth:")
    for r in report["detection_recall_vs_injected_ground_truth"]:
        print(f"    {r['defect_type']:<22} injected={r['injected']:>4} "
              f"detected={r['detected']:>4}  recall={r['recall_pct']}%")
    print(f"\n  wrote {out}")
    stop_spark()


if __name__ == "__main__":
    main()
