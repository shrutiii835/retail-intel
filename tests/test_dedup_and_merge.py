"""Tests for deduplication, Delta MERGE upsert behaviour and watermarks.

These cover the three mechanisms that stop the pipeline from double-counting:
collapse duplicates before the MERGE, upsert on the business key, and never
re-read a window that has already been committed.
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F


def _dedupe(df, keys, order_cols):
    """The same rule Silver applies: newest version of each business key wins."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def test_exact_duplicate_collapses_to_one_row(spark):
    df = spark.createDataFrame(
        [("TXN1", 3, "2025-02-01 10:00:00"), ("TXN1", 3, "2025-02-01 10:00:00")],
        "transaction_id STRING, quantity INT, last_modified STRING",
    )
    out = _dedupe(df, ["transaction_id"], ["last_modified"])
    assert out.count() == 1
    assert out.collect()[0]["quantity"] == 3


def test_dedup_keeps_the_newest_version(spark):
    """Two versions of one transaction: the later correction must win."""
    df = spark.createDataFrame(
        [
            ("TXN1", 5, "2025-02-01 10:00:00"),   # original
            ("TXN1", 2, "2025-02-09 08:15:00"),   # correction
        ],
        "transaction_id STRING, quantity INT, last_modified STRING",
    )
    out = _dedupe(df, ["transaction_id"], ["last_modified"]).collect()
    assert len(out) == 1
    assert out[0]["quantity"] == 2, "dedup kept the stale version"


def test_dedup_preserves_distinct_transactions(spark):
    df = spark.createDataFrame(
        [("TXN1", 1, "2025-02-01 10:00:00"), ("TXN2", 1, "2025-02-01 10:00:00")],
        "transaction_id STRING, quantity INT, last_modified STRING",
    )
    assert _dedupe(df, ["transaction_id"], ["last_modified"]).count() == 2


def test_inventory_dedup_uses_the_composite_grain(spark):
    """Inventory's business key is (date, product, store), not inventory_id."""
    df = spark.createDataFrame(
        [
            ("2025-02-02", "P1", "S1", 10, "2025-02-02 23:00:00"),
            ("2025-02-02", "P1", "S1", 12, "2025-02-03 06:00:00"),  # restated
            ("2025-02-02", "P1", "S2", 7, "2025-02-02 23:00:00"),
        ],
        "snapshot_date STRING, product_id STRING, store_id STRING, "
        "stock_quantity INT, last_modified STRING",
    )
    out = _dedupe(df, ["snapshot_date", "product_id", "store_id"], ["last_modified"])
    assert out.count() == 2
    restated = out.where("store_id = 'S1'").collect()[0]
    assert restated["stock_quantity"] == 12


# ---------------------------------------------------------------------------
# Delta MERGE
# ---------------------------------------------------------------------------

def _merge(spark, path, incoming):
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(incoming.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll(condition="s.last_modified > t.last_modified")
        .whenNotMatchedInsertAll()
        .execute()
    )


def test_merge_updates_in_place_rather_than_appending(spark, tmp_path):
    path = str(tmp_path / "fact")
    schema = "transaction_id STRING, quantity INT, last_modified STRING"

    spark.createDataFrame([("TXN1", 5, "2025-02-01 10:00:00")], schema) \
        .write.format("delta").mode("overwrite").save(path)

    correction = spark.createDataFrame([("TXN1", 2, "2025-02-09 08:15:00")], schema)
    _merge(spark, path, correction)

    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 1, "MERGE appended a duplicate instead of updating"
    assert rows[0]["quantity"] == 2


def test_merge_inserts_genuinely_new_rows(spark, tmp_path):
    path = str(tmp_path / "fact")
    schema = "transaction_id STRING, quantity INT, last_modified STRING"
    spark.createDataFrame([("TXN1", 5, "2025-02-01 10:00:00")], schema) \
        .write.format("delta").mode("overwrite").save(path)

    _merge(spark, path, spark.createDataFrame([("TXN2", 1, "2025-02-02 10:00:00")], schema))
    assert spark.read.format("delta").load(path).count() == 2


def test_replaying_an_old_batch_does_not_revert_a_newer_correction(spark, tmp_path):
    """The `s.last_modified > t.last_modified` guard earns its keep here.

    Without it, re-running an old batch would overwrite a later correction with
    stale values and silently corrupt the table.
    """
    path = str(tmp_path / "fact")
    schema = "transaction_id STRING, quantity INT, last_modified STRING"
    spark.createDataFrame([("TXN1", 5, "2025-02-01 10:00:00")], schema) \
        .write.format("delta").mode("overwrite").save(path)

    _merge(spark, path, spark.createDataFrame([("TXN1", 2, "2025-02-09 08:15:00")], schema))
    # Now replay the ORIGINAL, older version.
    _merge(spark, path, spark.createDataFrame([("TXN1", 5, "2025-02-01 10:00:00")], schema))

    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 1
    assert rows[0]["quantity"] == 2, "stale replay overwrote a newer correction"


def test_merge_is_idempotent_when_run_twice(spark, tmp_path):
    path = str(tmp_path / "fact")
    schema = "transaction_id STRING, quantity INT, last_modified STRING"
    spark.createDataFrame([("TXN1", 5, "2025-02-01 10:00:00")], schema) \
        .write.format("delta").mode("overwrite").save(path)

    batch = spark.createDataFrame(
        [("TXN2", 3, "2025-02-05 09:00:00"), ("TXN3", 4, "2025-02-05 09:30:00")], schema
    )
    _merge(spark, path, batch)
    first = spark.read.format("delta").load(path).count()
    _merge(spark, path, batch)
    second = spark.read.format("delta").load(path).count()

    assert first == second == 3


# ---------------------------------------------------------------------------
# Watermark behaviour
# ---------------------------------------------------------------------------

def test_watermark_predicate_excludes_already_processed_rows(spark):
    df = spark.createDataFrame(
        [("A", "2025-03-01 00:00:00"), ("B", "2025-03-05 00:00:00"),
         ("C", "2025-03-10 00:00:00")],
        "id STRING, last_modified STRING",
    )
    watermark = "2025-03-05 00:00:00"
    new_rows = df.where(F.col("last_modified") > F.lit(watermark))
    ids = {r["id"] for r in new_rows.collect()}
    assert ids == {"C"}, "watermark let an already-processed row through"


def test_watermark_advances_to_the_maximum_seen(spark):
    df = spark.createDataFrame(
        [("A", "2025-03-01 00:00:00"), ("B", "2025-03-10 00:00:00")],
        "id STRING, last_modified STRING",
    )
    assert df.agg(F.max("last_modified")).collect()[0][0] == "2025-03-10 00:00:00"


def test_empty_batch_leaves_the_watermark_untouched(spark):
    """If nothing arrives, the watermark must not move — otherwise a later
    late-arriving row inside that window would be skipped forever."""
    df = spark.createDataFrame([], "id STRING, last_modified STRING")
    new_max = df.agg(F.max("last_modified")).collect()[0][0]
    assert new_max is None
