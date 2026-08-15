"""Control plane: watermarks, run audit log and data-quality results.

Three small Delta tables under `_control/` carry the pipeline's own state. They
are what make the pipeline restartable and auditable rather than a script you
hope finished.

  watermarks     one row per (layer, table). The high-water mark of
                 last_modified_timestamp that has been *successfully* processed.
                 Written only after the load for that table commits — if the
                 load fails the watermark stays where it was and the next run
                 re-reads the same window. That is deliberate: re-reading is
                 safe because every downstream write is a MERGE on a business
                 key, so replay converges to the same result.

  pipeline_runs  one row per (run_id, layer, table) with row counts, status and
                 duration. This is the evidence behind the metrics in METRICS.md.

  dq_results     one row per (run_id, table, rule) with pass/fail counts. This
                 is what DATA_QUALITY.md reports and what the consistency
                 percentage is computed from.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.project_config import (  # noqa: E402
    DQ_RESULTS_TABLE,
    RUN_LOG_TABLE,
    WATERMARK_TABLE,
)
from src.common.spark_session import get_spark  # noqa: E402

# A watermark low enough that the first run reads everything.
EPOCH_WATERMARK = "1900-01-01 00:00:00"


def new_run_id() -> str:
    """A run id that sorts chronologically and is unique per execution."""
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Watermarks
# ---------------------------------------------------------------------------

_WM_SCHEMA = """
    layer STRING,
    table_name STRING,
    watermark_column STRING,
    watermark_value STRING,
    rows_last_load BIGINT,
    updated_at STRING,
    run_id STRING
"""


def _ensure_table(path: str, schema_ddl: str, partition_by: str | None = None) -> None:
    spark = get_spark()
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    empty = spark.createDataFrame([], schema=schema_ddl)
    writer = empty.write.format("delta")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.mode("overwrite").save(path)


def get_watermark(layer: str, table_name: str) -> str:
    """Current high-water mark, or the epoch value if this table is new."""
    spark = get_spark()
    _ensure_table(WATERMARK_TABLE, _WM_SCHEMA)
    row = (
        spark.read.format("delta")
        .load(WATERMARK_TABLE)
        .where(f"layer = '{layer}' AND table_name = '{table_name}'")
        .orderBy("watermark_value", ascending=False)
        .limit(1)
        .collect()
    )
    return row[0]["watermark_value"] if row else EPOCH_WATERMARK


def set_watermark(
    layer: str,
    table_name: str,
    watermark_column: str,
    watermark_value: str,
    rows_last_load: int,
    run_id: str,
) -> None:
    """Advance the watermark. Called *only* after the data write has committed.

    Uses MERGE so the control table holds exactly one current row per
    (layer, table) instead of growing an append-only history we would then have
    to filter on every read.
    """
    spark = get_spark()
    from delta.tables import DeltaTable

    _ensure_table(WATERMARK_TABLE, _WM_SCHEMA)
    updates = spark.createDataFrame(
        [(layer, table_name, watermark_column, watermark_value,
          int(rows_last_load), utc_now_str(), run_id)],
        schema=_WM_SCHEMA,
    )
    (
        DeltaTable.forPath(spark, WATERMARK_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.layer = s.layer AND t.table_name = s.table_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def reset_watermarks() -> None:
    """Wipe the control tables — used by `--full-reload` and by the tests."""
    spark = get_spark()
    for path, schema in (
        (WATERMARK_TABLE, _WM_SCHEMA),
        (RUN_LOG_TABLE, _RUN_SCHEMA),
        (DQ_RESULTS_TABLE, _DQ_SCHEMA),
    ):
        spark.createDataFrame([], schema=schema).write.format("delta").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(path)


# ---------------------------------------------------------------------------
# Run audit log
# ---------------------------------------------------------------------------

_RUN_SCHEMA = """
    run_id STRING,
    batch_id STRING,
    layer STRING,
    table_name STRING,
    status STRING,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_rejected BIGINT,
    rows_duplicate BIGINT,
    watermark_from STRING,
    watermark_to STRING,
    started_at STRING,
    finished_at STRING,
    duration_seconds DOUBLE,
    message STRING
"""


def log_run(**kwargs) -> None:
    """Append one audit row. Unknown keys default to None/0."""
    spark = get_spark()
    _ensure_table(RUN_LOG_TABLE, _RUN_SCHEMA)
    defaults = {
        "run_id": None, "batch_id": None, "layer": None, "table_name": None,
        "status": None, "rows_read": 0, "rows_written": 0, "rows_inserted": 0,
        "rows_updated": 0, "rows_rejected": 0, "rows_duplicate": 0,
        "watermark_from": None, "watermark_to": None, "started_at": None,
        "finished_at": None, "duration_seconds": 0.0, "message": None,
    }
    defaults.update(kwargs)
    order = [c.strip().split()[0] for c in _RUN_SCHEMA.strip().split(",")]
    row = tuple(defaults[c] for c in order)
    spark.createDataFrame([row], schema=_RUN_SCHEMA).write.format("delta").mode(
        "append"
    ).save(RUN_LOG_TABLE)


# ---------------------------------------------------------------------------
# Data-quality results
# ---------------------------------------------------------------------------

_DQ_SCHEMA = """
    run_id STRING,
    batch_id STRING,
    layer STRING,
    table_name STRING,
    rule_id STRING,
    rule_description STRING,
    severity STRING,
    rows_evaluated BIGINT,
    rows_failed BIGINT,
    fail_pct DOUBLE,
    recorded_at STRING
"""


def log_dq_results(rows: list[dict]) -> None:
    if not rows:
        return
    spark = get_spark()
    _ensure_table(DQ_RESULTS_TABLE, _DQ_SCHEMA)
    order = [c.strip().split()[0] for c in _DQ_SCHEMA.strip().split(",")]
    tuples = [tuple(r.get(c) for c in order) for r in rows]
    spark.createDataFrame(tuples, schema=_DQ_SCHEMA).write.format("delta").mode(
        "append"
    ).save(DQ_RESULTS_TABLE)
