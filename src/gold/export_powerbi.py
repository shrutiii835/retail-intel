"""Export the Gold layer to files Power BI Desktop can import.

Why export at all? Power BI Desktop cannot read a Delta table from a local
folder — the Delta connector targets Databricks/Fabric, and pointing Power BI at
the underlying Parquet files would make it read every historical version in
_delta_log rather than the current snapshot. Exporting the current snapshot to
plain CSV is the cheapest correct answer, and it keeps Databricks compute
switched off while the report is being built (see AZURE_CLEANUP.md).

In a production deployment you would instead point Power BI at Databricks SQL
using the native connector and skip this step entirely. DECISIONS.md records
that trade-off.

    python -m src.gold.export_powerbi
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from config.project_config import GOLD_TABLES, REPO_ROOT  # noqa: E402
from src.common.spark_session import get_spark, stop_spark  # noqa: E402
from src.gold.kpis import MART_CAMPAIGN_ROI  # noqa: E402

EXPORT_DIR = Path(REPO_ROOT) / "powerbi" / "data"

# Lineage/audit columns are deliberately dropped: they are operational metadata,
# and shipping them into the semantic model invites someone to build a measure
# on top of a run id.
DROP_PREFIXES = ("_gold_run_id", "_silver_run_id", "_ingest_run_id", "_batch_id",
                 "_silver_processed_at", "_row_hash")


def _export(name: str, path: str) -> dict:
    spark = get_spark()
    df = spark.read.format("delta").load(path)
    keep = [c for c in df.columns if c not in DROP_PREFIXES]
    df = df.select(*keep)

    tmp = EXPORT_DIR / f"_tmp_{name}"
    # coalesce(1) so Power BI gets one file per table rather than a folder of
    # part-files. Safe here: the largest table is ~130K rows.
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp))

    part = next(p for p in tmp.glob("part-*.csv"))
    target = EXPORT_DIR / f"{name}.csv"
    if target.exists():
        target.unlink()
    shutil.move(str(part), str(target))
    shutil.rmtree(tmp)

    return {"table": name, "rows": df.count(), "columns": len(keep),
            "file": str(target.relative_to(REPO_ROOT))}


def main() -> None:
    get_spark()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    targets = {
        "dim_date": GOLD_TABLES["dim_date"],
        "dim_product": GOLD_TABLES["dim_product"],
        "dim_store": GOLD_TABLES["dim_store"],
        "dim_campaign": GOLD_TABLES["dim_campaign"],
        "fact_sales": GOLD_TABLES["fact_sales"],
        "fact_inventory_snapshot": GOLD_TABLES["fact_inventory_snapshot"],
        "mart_campaign_roi": MART_CAMPAIGN_ROI,
    }

    results = []
    print("exporting Gold layer for Power BI:")
    for name, path in targets.items():
        r = _export(name, path)
        results.append(r)
        print(f"  {r['table']:<26} rows={r['rows']:>7,}  cols={r['columns']:>3}  → {r['file']}")

    manifest = {"exported": results, "export_dir": str(EXPORT_DIR.relative_to(REPO_ROOT))}
    (EXPORT_DIR / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"\nwrote {EXPORT_DIR}/_manifest.json")
    stop_spark()


if __name__ == "__main__":
    main()
