"""Bronze layer — raw ingestion with a watermark predicate.

This module is the local stand-in for what the ADF Copy activity does in Azure.
ADF issues, in effect:

    SELECT * FROM <source> WHERE last_modified_timestamp > '@{watermark}'

and drops the result into ADLS. Here we read the landing CSV (which represents
the source system table), apply exactly that predicate, stamp lineage metadata
on every row, and append to a Delta table under bronze/. The predicate is the
same in both places, so the behaviour demonstrated locally is the behaviour
running in Azure.

What Bronze deliberately does NOT do
------------------------------------
No type casting, no cleaning, no deduplication, no business rules. Every column
lands as STRING. Two reasons:

  1. A cast that fails in Bronze destroys the evidence. "N/A" cast to INT is
     just NULL, and now nobody can tell whether the source sent "N/A", an empty
     string, or a genuine null. Keeping the raw text means Silver can *report*
     precisely what arrived and quarantine it with the original value attached.
  2. Reprocessing. If a Silver rule turns out to be wrong, we rebuild Silver
     from Bronze rather than going back to the source system — which in a real
     estate may have already overwritten the row.

Idempotency
-----------
Bronze is partitioned by _batch_id and written with replaceWhere, so re-running
the same batch replaces that partition rather than appending a second copy.
Combined with the watermark (which returns zero rows on an unchanged rerun),
running this twice is a no-op.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402

from config.project_config import (  # noqa: E402
    BRONZE_TABLES,
    LANDING_ROOT,
)
from src.common.control import (  # noqa: E402
    get_watermark,
    log_run,
    set_watermark,
    utc_now_str,
)
from src.common.schemas import CONTRACTS, validate_structure  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402

LAYER = "bronze"

# Order matters: reference data is ingested before the transactional feeds so
# that Silver's referential-integrity rules always have a master to check
# against, even on the very first run.
SOURCE_ORDER = ("products", "stores", "campaigns", "sales", "inventory")


def _landing_path(batch_id: str, source: str) -> str:
    # String concatenation, not Path(): on Databricks LANDING_ROOT is an
    # abfss:// URL and Path() would collapse the double slash into one.
    return f"{LANDING_ROOT}/{batch_id}/{source}"


def ingest_source(
    source: str,
    batch_id: str,
    run_id: str,
    full_reload: bool = False,
) -> dict:
    """Ingest one source feed for one batch. Returns a stats dict."""
    spark = get_spark()
    contract = CONTRACTS[source]
    started = time.time()
    started_at = utc_now_str()
    target = BRONZE_TABLES[source]
    landing = _landing_path(batch_id, source)

    watermark_from = "1900-01-01 00:00:00" if full_reload else get_watermark(LAYER, source)

    # Everything as STRING — inferSchema is off on purpose (see module docstring).
    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(landing)
    )

    # Contract check before anything else. A missing column is a pipeline
    # failure, not a data-quality statistic.
    validate_structure(raw, contract, source_label=f"{batch_id}/{source}")

    rows_available = raw.count()

    wm_col = contract.watermark_column
    # The incremental predicate. Strictly greater-than: rows at exactly the
    # watermark were, by construction, committed in a previous run. (In a
    # source with concurrent writers you would use >= plus a dedupe on the
    # business key; our MERGE in Silver makes that safe either way.)
    incoming = raw.where(F.col(wm_col) > F.lit(watermark_from))

    enriched = (
        incoming.withColumn("_ingest_run_id", F.lit(run_id))
        .withColumn("_ingest_timestamp", F.lit(utc_now_str()).cast("timestamp"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_system", F.lit(f"retail_{source}"))
        .withColumn("_batch_id", F.lit(batch_id))
    )
    enriched = enriched.cache()
    rows_ingested = enriched.count()

    if rows_ingested == 0:
        enriched.unpersist()
        duration = time.time() - started
        log_run(
            run_id=run_id, batch_id=batch_id, layer=LAYER, table_name=source,
            status="SUCCESS_NO_DATA", rows_read=rows_available, rows_written=0,
            watermark_from=watermark_from, watermark_to=watermark_from,
            started_at=started_at, finished_at=utc_now_str(),
            duration_seconds=round(duration, 3),
            message="Watermark predicate matched no new or changed rows.",
        )
        return {
            "source": source, "rows_available": rows_available, "rows_ingested": 0,
            "watermark_from": watermark_from, "watermark_to": watermark_from,
            "status": "SUCCESS_NO_DATA",
        }

    new_watermark = enriched.agg(F.max(wm_col)).collect()[0][0]

    from delta.tables import DeltaTable

    writer = enriched.write.format("delta").partitionBy("_batch_id")
    if DeltaTable.isDeltaTable(spark, target) and not full_reload:
        # replaceWhere gives us an atomic per-batch overwrite: rerunning a batch
        # replaces its partition instead of doubling it.
        writer = writer.mode("overwrite").option("replaceWhere", f"_batch_id = '{batch_id}'")
    else:
        writer = writer.mode("overwrite").option("overwriteSchema", "true")
    writer.save(target)

    # Watermark advances ONLY after the write above has committed.
    set_watermark(LAYER, source, wm_col, str(new_watermark), rows_ingested, run_id)

    enriched.unpersist()
    duration = time.time() - started
    log_run(
        run_id=run_id, batch_id=batch_id, layer=LAYER, table_name=source,
        status="SUCCESS", rows_read=rows_available, rows_written=rows_ingested,
        rows_inserted=rows_ingested,
        watermark_from=watermark_from, watermark_to=str(new_watermark),
        started_at=started_at, finished_at=utc_now_str(),
        duration_seconds=round(duration, 3),
        message=f"Ingested {rows_ingested} of {rows_available} available rows.",
    )
    return {
        "source": source,
        "rows_available": rows_available,
        "rows_ingested": rows_ingested,
        "rows_filtered_by_watermark": rows_available - rows_ingested,
        "watermark_from": watermark_from,
        "watermark_to": str(new_watermark),
        "status": "SUCCESS",
    }


def ingest_batch(batch_id: str, run_id: str, full_reload: bool = False) -> list[dict]:
    results = []
    for source in SOURCE_ORDER:
        stats = ingest_source(source, batch_id, run_id, full_reload=full_reload)
        results.append(stats)
        print(
            f"    bronze/{source:<10} available={stats['rows_available']:>7,} "
            f"ingested={stats['rows_ingested']:>7,} "
            f"watermark→{stats['watermark_to']}"
        )
    return results
