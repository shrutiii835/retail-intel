"""Expected schemas and declarative data-quality rules.

Two different kinds of check live here, and keeping them apart matters:

  STRUCTURAL check  — "does the file even have the columns we agreed on?"
                      A missing or renamed column is a *contract breach*. It is
                      not one bad row, it is the whole feed being wrong, so it
                      raises SchemaValidationError and fails the pipeline. Left
                      unchecked, a renamed column silently nulls a whole measure.

  ROW-LEVEL rule    — "is this particular row usable?"
                      A negative quantity is bad *data*, not a broken pipeline.
                      These rows are quarantined with their reasons attached and
                      the run continues.

Rules are declarative (a rule id, a description, a severity and a Spark SQL
expression that is TRUE when the row is valid) so the same list drives three
things at once: the filter that splits clean from quarantined rows, the
per-rule counts written to the dq_results control table, and the rule
documentation in DATA_QUALITY.md.
"""

from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Severities
# ---------------------------------------------------------------------------
# REJECT  row cannot be trusted → quarantine, never reaches Silver.
# REPAIR  row is usable once a defined substitution is applied → stays, and the
#         substitution is recorded so the effect is visible in the metrics.
REJECT = "reject"
REPAIR = "repair"


class SchemaValidationError(Exception):
    """Raised when an incoming feed does not carry the agreed columns."""


@dataclass(frozen=True)
class Rule:
    rule_id: str
    description: str
    severity: str
    expression: str          # Spark SQL — TRUE means the row PASSES


@dataclass(frozen=True)
class SourceContract:
    name: str
    required_columns: tuple[str, ...]
    business_key: tuple[str, ...]
    watermark_column: str
    rules: tuple[Rule, ...]


# ---------------------------------------------------------------------------
# SALES
# ---------------------------------------------------------------------------
# Note the casts: Bronze stores everything as STRING on purpose (see
# ARCHITECTURE.md), so Silver is the first place types are asserted. try_cast
# returns NULL instead of throwing, which lets a bad value be *reported* as a
# failed rule rather than killing the job.

SALES_CONTRACT = SourceContract(
    name="sales",
    required_columns=(
        "transaction_id", "transaction_timestamp", "product_id", "store_id",
        "quantity", "unit_price", "discount_amount", "campaign_id",
        "last_modified_timestamp",
    ),
    business_key=("transaction_id",),
    watermark_column="last_modified_timestamp",
    rules=(
        Rule("SAL_001", "transaction_id must be present", REJECT,
             "transaction_id IS NOT NULL AND trim(transaction_id) <> ''"),
        Rule("SAL_002", "product_id must be present", REJECT,
             "product_id IS NOT NULL AND trim(product_id) <> ''"),
        Rule("SAL_003", "store_id must be present", REJECT,
             "store_id IS NOT NULL AND trim(store_id) <> ''"),
        Rule("SAL_004", "transaction_timestamp must parse to a timestamp", REJECT,
             "transaction_timestamp_ts IS NOT NULL"),
        Rule("SAL_005", "quantity must parse to a whole number", REJECT,
             "quantity_int IS NOT NULL"),
        Rule("SAL_006", "quantity must be greater than zero", REJECT,
             "quantity_int > 0"),
        Rule("SAL_007", "unit_price must parse to a number", REJECT,
             "unit_price_dec IS NOT NULL"),
        Rule("SAL_008", "unit_price must not be negative", REJECT,
             "unit_price_dec >= 0"),
        Rule("SAL_009", "discount_amount must not be negative", REJECT,
             "discount_amount_dec >= 0"),
        Rule("SAL_010", "discount must not exceed the gross line amount", REJECT,
             "discount_amount_dec <= (quantity_int * unit_price_dec) + 0.01"),
        Rule("SAL_011", "transaction date must fall inside the reporting period", REJECT,
             "transaction_timestamp_ts >= to_timestamp('{period_start} 00:00:00') "
             "AND transaction_timestamp_ts < to_timestamp('{period_end_exclusive} 00:00:00')"),
        Rule("SAL_012", "product_id must exist in the product master", REJECT,
             "product_exists = true"),
        Rule("SAL_013", "store_id must exist in the store master", REJECT,
             "store_exists = true"),
        # Repair, not reject: a sale genuinely happened and its revenue must
        # still be counted. Only the campaign attribution is unknown, so the row
        # is routed to the Unknown campaign member and counted as a consistency
        # defect in METRICS.md rather than being thrown away.
        Rule("SAL_014", "campaign_id must exist in the campaign master when supplied", REPAIR,
             "campaign_exists = true"),
    ),
)

# ---------------------------------------------------------------------------
# INVENTORY
# ---------------------------------------------------------------------------

INVENTORY_CONTRACT = SourceContract(
    name="inventory",
    required_columns=(
        "inventory_id", "snapshot_date", "product_id", "store_id",
        "stock_quantity", "last_modified_timestamp",
    ),
    business_key=("snapshot_date", "product_id", "store_id"),
    watermark_column="last_modified_timestamp",
    rules=(
        Rule("INV_001", "inventory_id must be present", REJECT,
             "inventory_id IS NOT NULL AND trim(inventory_id) <> ''"),
        Rule("INV_002", "snapshot_date must parse to a date", REJECT,
             "snapshot_date_dt IS NOT NULL"),
        Rule("INV_003", "product_id must be present", REJECT,
             "product_id IS NOT NULL AND trim(product_id) <> ''"),
        Rule("INV_004", "store_id must be present", REJECT,
             "store_id IS NOT NULL AND trim(store_id) <> ''"),
        Rule("INV_005", "stock_quantity must parse to a whole number", REJECT,
             "stock_quantity_int IS NOT NULL"),
        Rule("INV_006", "stock_quantity must not be negative", REJECT,
             "stock_quantity_int >= 0"),
        Rule("INV_007", "product_id must exist in the product master", REJECT,
             "product_exists = true"),
        Rule("INV_008", "store_id must exist in the store master", REJECT,
             "store_exists = true"),
    ),
)

# ---------------------------------------------------------------------------
# CAMPAIGNS
# ---------------------------------------------------------------------------

CAMPAIGN_CONTRACT = SourceContract(
    name="campaigns",
    required_columns=(
        "campaign_id", "campaign_name", "product_id", "start_date", "end_date",
        "discount_percentage", "campaign_cost", "last_modified_timestamp",
    ),
    business_key=("campaign_id",),
    watermark_column="last_modified_timestamp",
    rules=(
        Rule("CAM_001", "campaign_id must be present", REJECT,
             "campaign_id IS NOT NULL AND trim(campaign_id) <> ''"),
        Rule("CAM_002", "start_date must parse to a date", REJECT,
             "start_date_dt IS NOT NULL"),
        Rule("CAM_003", "end_date must parse to a date", REJECT,
             "end_date_dt IS NOT NULL"),
        Rule("CAM_004", "end_date must not precede start_date", REJECT,
             "end_date_dt >= start_date_dt"),
        Rule("CAM_005", "discount_percentage must be between 0 and 100", REJECT,
             "discount_percentage_dec BETWEEN 0 AND 100"),
        Rule("CAM_006", "campaign_cost must not be negative", REJECT,
             "campaign_cost_dec >= 0"),
        Rule("CAM_007", "product_id must exist in the product master", REJECT,
             "product_exists = true"),
    ),
)

# ---------------------------------------------------------------------------
# PRODUCTS
# ---------------------------------------------------------------------------

PRODUCT_CONTRACT = SourceContract(
    name="products",
    required_columns=(
        "product_id", "product_name", "category", "sub_category", "brand",
        "unit_cost", "list_price", "last_modified_timestamp",
    ),
    business_key=("product_id",),
    watermark_column="last_modified_timestamp",
    rules=(
        Rule("PRD_001", "product_id must be present", REJECT,
             "product_id IS NOT NULL AND trim(product_id) <> ''"),
        Rule("PRD_002", "product_name must be present", REJECT,
             "product_name IS NOT NULL AND trim(product_name) <> ''"),
        Rule("PRD_003", "unit_cost must parse and be non-negative", REJECT,
             "unit_cost_dec IS NOT NULL AND unit_cost_dec >= 0"),
        Rule("PRD_004", "list_price must parse and be non-negative", REJECT,
             "list_price_dec IS NOT NULL AND list_price_dec >= 0"),
        Rule("PRD_005", "list_price must not be below unit_cost", REJECT,
             "list_price_dec >= unit_cost_dec"),
    ),
)

# ---------------------------------------------------------------------------
# STORES
# ---------------------------------------------------------------------------

STORE_CONTRACT = SourceContract(
    name="stores",
    required_columns=(
        "store_id", "store_name", "city", "state", "region", "store_format",
        "open_date", "last_modified_timestamp",
    ),
    business_key=("store_id",),
    watermark_column="last_modified_timestamp",
    rules=(
        Rule("STR_001", "store_id must be present", REJECT,
             "store_id IS NOT NULL AND trim(store_id) <> ''"),
        Rule("STR_002", "store_name must be present", REJECT,
             "store_name IS NOT NULL AND trim(store_name) <> ''"),
        Rule("STR_003", "region must be present", REJECT,
             "region IS NOT NULL AND trim(region) <> ''"),
        Rule("STR_004", "open_date must parse to a date", REJECT,
             "open_date_dt IS NOT NULL"),
    ),
)


CONTRACTS: dict[str, SourceContract] = {
    c.name: c
    for c in (
        SALES_CONTRACT, INVENTORY_CONTRACT, CAMPAIGN_CONTRACT,
        PRODUCT_CONTRACT, STORE_CONTRACT,
    )
}


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------

def validate_structure(df, contract: SourceContract, source_label: str) -> None:
    """Assert the feed carries every agreed column.

    Raises SchemaValidationError (a *pipeline* failure) rather than returning a
    row count, because there is no sensible partial result when the contract is
    broken.
    """
    present = {c.lower() for c in df.columns}
    missing = [c for c in contract.required_columns if c.lower() not in present]
    if missing:
        raise SchemaValidationError(
            f"[{source_label}] feed '{contract.name}' is missing required "
            f"column(s): {', '.join(missing)}. "
            f"Received columns: {', '.join(sorted(df.columns))}"
        )


def rules_for(name: str) -> tuple[Rule, ...]:
    return CONTRACTS[name].rules


def rule_columns(contract: SourceContract) -> list[str]:
    return [r.rule_id for r in contract.rules]
