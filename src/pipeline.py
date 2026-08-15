"""RetailIntel pipeline orchestrator.

Locally this plays the role Azure Data Factory plays in Azure: it decides which
batch runs, in which order the layers execute, and it stops the run if a layer
fails. The transformation logic itself lives in the layer modules, exactly as it
does in Azure — ADF orchestrates, Databricks transforms.

    python -m src.pipeline --batch batch_01_initial
    python -m src.pipeline --batch all
    python -m src.pipeline --batch all --full-reload
    python -m src.pipeline --batch batch_02_incremental --layers bronze,silver
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.project_config import INGESTION_BATCHES, METRICS_DIR  # noqa: E402
from src.bronze.ingest import ingest_batch  # noqa: E402
from src.common.control import new_run_id, reset_watermarks  # noqa: E402
from src.common.spark_session import get_spark, stop_spark  # noqa: E402
from src.silver.build_silver import build_silver  # noqa: E402

ALL_BATCHES = [b.batch_id for b in INGESTION_BATCHES]


def run_batch(batch_id: str, layers: list[str], full_reload: bool = False) -> dict:
    run_id = new_run_id()
    print(f"\n=== BATCH {batch_id}  (run_id={run_id}, full_reload={full_reload}) ===")
    started = time.time()
    result: dict = {"batch_id": batch_id, "run_id": run_id, "layers": {}}

    if "bronze" in layers:
        print("  [bronze] ingest")
        result["layers"]["bronze"] = ingest_batch(batch_id, run_id, full_reload=full_reload)
    if "silver" in layers:
        print("  [silver] validate → quarantine → dedupe → merge")
        result["layers"]["silver"] = build_silver(batch_id, run_id, full_reload=full_reload)
    if "gold" in layers:
        from src.gold.build_gold import build_gold

        print("  [gold]   dimensions → facts")
        result["layers"]["gold"] = build_gold(batch_id, run_id, full_reload=full_reload)

    result["duration_seconds"] = round(time.time() - started, 2)
    print(f"  batch complete in {result['duration_seconds']}s")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="RetailIntel medallion pipeline")
    ap.add_argument("--batch", default="all", help="batch id or 'all'")
    ap.add_argument("--layers", default="bronze,silver,gold")
    ap.add_argument(
        "--full-reload",
        action="store_true",
        help="ignore watermarks and rebuild every layer from scratch",
    )
    args = ap.parse_args()

    layers = [x.strip() for x in args.layers.split(",") if x.strip()]
    batches = ALL_BATCHES if args.batch == "all" else [args.batch]

    get_spark()
    if args.full_reload:
        print("full reload requested — clearing watermarks and control tables")
        reset_watermarks()

    results = []
    try:
        for i, b in enumerate(batches):
            # A full reload only applies to the first batch; the rest must run
            # incrementally or we would not be testing incremental behaviour.
            results.append(run_batch(b, layers, full_reload=args.full_reload and i == 0))
    finally:
        Path(METRICS_DIR).mkdir(parents=True, exist_ok=True)
        with open(Path(METRICS_DIR) / "last_pipeline_run.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
        stop_spark()

    print("\nall batches complete → metrics/last_pipeline_run.json")


if __name__ == "__main__":
    main()
