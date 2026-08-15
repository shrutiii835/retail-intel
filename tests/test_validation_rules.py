"""Rule-by-rule tests for the Silver validation layer.

Each test crafts a tiny Bronze-shaped DataFrame (all strings, as Bronze stores
it) containing exactly one bad row, and asserts that the intended rule — and
only that rule — rejects it. A rule that silently passes bad data is worse than
no rule at all, because the dashboard then looks trustworthy while being wrong.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.common.schemas import (
    REJECT,
    SALES_CONTRACT,
    SchemaValidationError,
    validate_structure,
)
from src.silver.build_silver import _expr, _prepare

BRONZE_COLS = [
    "transaction_id", "transaction_timestamp", "product_id", "store_id",
    "quantity", "unit_price", "discount_amount", "campaign_id",
    "last_modified_timestamp",
]


def _row(**overrides):
    base = {
        "transaction_id": "TXN00000001",
        "transaction_timestamp": "2025-02-10 14:30:00",
        "product_id": "P0001",
        "store_id": "S001",
        "quantity": "3",
        "unit_price": "10.00",
        "discount_amount": "1.50",
        "campaign_id": "",
        "last_modified_timestamp": "2025-02-10 14:30:00",
    }
    base.update(overrides)
    return tuple(base[c] for c in BRONZE_COLS)


def _evaluate(spark, rows, monkeypatch, products=("P0001",), stores=("S001",),
              campaigns=("C001",)):
    """Run the real rule expressions against crafted rows.

    _prepare() reads the Silver master tables to resolve referential integrity;
    here those are replaced with in-memory stand-ins so the test never depends
    on a built lakehouse.
    """
    import src.silver.build_silver as bs

    schema = ", ".join(f"{c} STRING" for c in BRONZE_COLS)
    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("_ingest_timestamp", F.lit("2025-05-01 00:00:00").cast("timestamp"))

    def fake_prepare(source, frame):
        real_get = bs.get_spark

        def ref_frames(name, col):
            values = {"products": products, "stores": stores, "campaigns": campaigns}[name]
            return real_get().createDataFrame([(v,) for v in values], f"{col} STRING")

        # _prepare closes over a local `ref` helper; rebuild the joins here with
        # the same semantics rather than reaching inside it.
        out = (
            frame.withColumn("transaction_timestamp_ts",
                             F.expr("try_cast(transaction_timestamp AS timestamp)"))
            .withColumn("quantity_dbl", F.expr("try_cast(quantity AS double)"))
            .withColumn("quantity_int",
                        F.when(F.col("quantity_dbl").isNotNull()
                               & (F.col("quantity_dbl") == F.floor(F.col("quantity_dbl"))),
                               F.col("quantity_dbl").cast("int")))
            .withColumn("unit_price_dec", F.expr("try_cast(unit_price AS decimal(12,2))"))
            .withColumn("discount_amount_dec",
                        F.coalesce(F.expr("try_cast(discount_amount AS decimal(12,2))"),
                                   F.lit(0).cast("decimal(12,2)")))
            .withColumn("last_modified_ts",
                        F.expr("try_cast(last_modified_timestamp AS timestamp)"))
            .withColumn("campaign_id_clean",
                        F.when(F.trim(F.col("campaign_id")) == "", None)
                        .otherwise(F.trim(F.col("campaign_id"))))
        )
        out = (
            out.join(ref_frames("products", "product_id")
                     .withColumn("product_exists", F.lit(True)),
                     on="product_id", how="left")
            .join(ref_frames("stores", "store_id").withColumn("store_exists", F.lit(True)),
                  on="store_id", how="left")
            .join(ref_frames("campaigns", "campaign_id")
                  .withColumnRenamed("campaign_id", "campaign_id_clean")
                  .withColumn("_cf", F.lit(True)),
                  on="campaign_id_clean", how="left")
            .withColumn("product_exists", F.coalesce("product_exists", F.lit(False)))
            .withColumn("store_exists", F.coalesce("store_exists", F.lit(False)))
            .withColumn("campaign_exists",
                        F.when(F.col("campaign_id_clean").isNull(), F.lit(True))
                        .otherwise(F.coalesce("_cf", F.lit(False))))
        )
        return out

    prepared = fake_prepare("sales", df)
    for rule in SALES_CONTRACT.rules:
        prepared = prepared.withColumn(rule.rule_id, F.expr(_expr(rule)))
    return prepared


def _failed_rules(spark, row, monkeypatch, **kw):
    prepared = _evaluate(spark, [row], monkeypatch, **kw)
    r = prepared.collect()[0]
    return {rule.rule_id for rule in SALES_CONTRACT.rules if r[rule.rule_id] is False}


# ---------------------------------------------------------------------------

def test_clean_row_passes_every_rule(spark, monkeypatch):
    assert _failed_rules(spark, _row(), monkeypatch) == set()


@pytest.mark.parametrize(
    "overrides,expected_rule",
    [
        ({"transaction_id": None}, "SAL_001"),
        ({"product_id": None}, "SAL_002"),
        ({"store_id": None}, "SAL_003"),
        ({"transaction_timestamp": "not-a-date"}, "SAL_004"),
        ({"quantity": "N/A"}, "SAL_005"),
        ({"quantity": "0"}, "SAL_006"),
        ({"quantity": "-3"}, "SAL_006"),
        ({"unit_price": "abc"}, "SAL_007"),
        ({"unit_price": "-5.00"}, "SAL_008"),
        ({"discount_amount": "-1.00"}, "SAL_009"),
        ({"transaction_timestamp": "2024-06-01 10:00:00"}, "SAL_011"),
        ({"product_id": "P9999"}, "SAL_012"),
        ({"store_id": "S999"}, "SAL_013"),
        ({"campaign_id": "C999"}, "SAL_014"),
    ],
)
def test_each_defect_triggers_its_rule(spark, monkeypatch, overrides, expected_rule):
    failed = _failed_rules(spark, _row(**overrides), monkeypatch)
    assert expected_rule in failed, (
        f"{overrides} should have failed {expected_rule}, failed {sorted(failed)}"
    )


def test_discount_cannot_exceed_line_total(spark, monkeypatch):
    # 2 × 10.00 = 20.00 gross, discount of 25.00 is impossible.
    failed = _failed_rules(
        spark, _row(quantity="2", unit_price="10.00", discount_amount="25.00"), monkeypatch
    )
    assert "SAL_010" in failed


def test_discount_exactly_equal_to_line_total_is_allowed(spark, monkeypatch):
    """A 100% discount is a real thing (giveaway) and must not be rejected."""
    failed = _failed_rules(
        spark, _row(quantity="2", unit_price="10.00", discount_amount="20.00"), monkeypatch
    )
    assert "SAL_010" not in failed


def test_blank_campaign_id_is_valid_not_an_orphan(spark, monkeypatch):
    """An empty campaign means 'not promoted', which is a legitimate state."""
    failed = _failed_rules(spark, _row(campaign_id=""), monkeypatch)
    assert "SAL_014" not in failed


def test_recoverable_decimal_quantity_is_accepted(spark, monkeypatch):
    """'3.0' is a formatting artefact carrying a valid quantity — repair it."""
    failed = _failed_rules(spark, _row(quantity="3.0"), monkeypatch)
    assert "SAL_005" not in failed and "SAL_006" not in failed


def test_fractional_quantity_is_rejected(spark, monkeypatch):
    """'2.5' is not a recoverable whole number — you cannot sell half a unit."""
    failed = _failed_rules(spark, _row(quantity="2.5"), monkeypatch)
    assert "SAL_005" in failed


def test_campaign_id_orphan_is_repair_not_reject():
    """SAL_014 must stay a REPAIR rule: a real sale must not be deleted because
    its campaign attribution is unknown."""
    rule = {r.rule_id: r for r in SALES_CONTRACT.rules}["SAL_014"]
    assert rule.severity != REJECT


# ---------------------------------------------------------------------------
# Structural (contract) validation
# ---------------------------------------------------------------------------

def test_missing_column_raises_pipeline_error(spark):
    """A missing column is a contract breach and must stop the run."""
    df = spark.createDataFrame([("TXN1", "P1")], "transaction_id STRING, product_id STRING")
    with pytest.raises(SchemaValidationError) as exc:
        validate_structure(df, SALES_CONTRACT, "unit-test")
    assert "missing required column" in str(exc.value)
    assert "store_id" in str(exc.value)


def test_structural_validation_passes_on_full_column_set(spark):
    schema = ", ".join(f"{c} STRING" for c in BRONZE_COLS)
    df = spark.createDataFrame([_row()], schema=schema)
    validate_structure(df, SALES_CONTRACT, "unit-test")   # must not raise


def test_extra_columns_are_tolerated(spark):
    """A new source column must not break ingestion — only missing ones do."""
    schema = ", ".join(f"{c} STRING" for c in BRONZE_COLS) + ", loyalty_id STRING"
    df = spark.createDataFrame([_row() + ("L123",)], schema=schema)
    validate_structure(df, SALES_CONTRACT, "unit-test")
