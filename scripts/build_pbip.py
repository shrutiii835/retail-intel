"""Generate a Power BI Project (PBIP) for RetailIntel.

Why PBIP and not `.pbix`: a `.pbix` embeds a *compiled* tabular database that
only the Analysis Services engine can write, so it cannot be produced by a
script. PBIP is Power BI's text-based project format — the semantic model is
TMSL/JSON — which means the whole model (tables, types, relationships, measures)
can be generated here and opened directly in Power BI Desktop.

What this builds:

    powerbi/RetailIntel.pbip                    project pointer
    powerbi/RetailIntel.SemanticModel/          tables, relationships, measures
    powerbi/RetailIntel.Report/                 four named pages

The report pages are created **empty on purpose**. Hand-authoring Power BI's
visual-container JSON is fragile, and it cannot be validated without Power BI
Desktop — which does not run on macOS, where this was written. A malformed
report definition stops the whole project opening, which would be worse than no
visuals at all. Placing visuals on a ready-made model takes about twenty
minutes; rebuilding a broken model takes far longer.

    python scripts/build_pbip.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PBI = REPO / "powerbi"
MODEL_DIR = PBI / "RetailIntel.SemanticModel"
REPORT_DIR = PBI / "RetailIntel.Report"

# --------------------------------------------------------------------------
# Column typing — must match the exported CSVs exactly
# --------------------------------------------------------------------------
# TMSL dataType values: string | int64 | double | decimal | dateTime | boolean
# `decimal` is used for money so Power BI does not introduce floating-point
# drift in totals that must reconcile against the Gold layer.

S, I, D, DEC, DT, B = "string", "int64", "double", "decimal", "dateTime", "boolean"

TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_date": [
        ("date_key", I), ("date", DT), ("year", I), ("quarter", I), ("month", I),
        ("month_name", S), ("year_month", I), ("year_month_label", S),
        ("day_of_month", I), ("day_of_week", I), ("day_name", S),
        ("week_of_year", I), ("is_weekend", B),
    ],
    "dim_product": [
        ("product_key", I), ("product_id", S), ("product_name", S),
        ("category", S), ("sub_category", S), ("brand", S),
        ("unit_cost", DEC), ("list_price", DEC), ("gross_margin_pct", D),
    ],
    "dim_store": [
        ("store_key", I), ("store_id", S), ("store_name", S), ("city", S),
        ("state", S), ("region", S), ("store_format", S), ("open_date", DT),
    ],
    "dim_campaign": [
        ("campaign_key", I), ("campaign_id", S), ("campaign_name", S),
        ("product_id", S), ("start_date", DT), ("end_date", DT),
        ("discount_percentage", DEC), ("campaign_cost", DEC), ("duration_days", I),
    ],
    "fact_sales": [
        ("transaction_id", S), ("date_key", I), ("product_key", I),
        ("store_key", I), ("campaign_key", I), ("transaction_timestamp", DT),
        ("transaction_date", DT), ("year_month", I), ("quantity", I),
        ("unit_price", DEC), ("gross_amount", DEC), ("discount_amount", DEC),
        ("net_revenue", DEC), ("cogs", DEC), ("gross_profit", DEC),
        ("is_promoted", B), ("campaign_resolved", B), ("last_modified_timestamp", DT),
    ],
    "fact_inventory_snapshot": [
        ("date_key", I), ("product_key", I), ("store_key", I),
        ("snapshot_date", DT), ("year_month", I), ("inventory_id", S),
        ("stock_quantity", I), ("stock_value_at_cost", DEC),
        ("last_modified_timestamp", DT),
    ],
    "mart_campaign_roi": [
        ("campaign_key", I), ("campaign_id", S), ("campaign_name", S),
        ("product_key", I), ("product_id", S), ("product_name", S),
        ("category", S), ("brand", S), ("start_date", DT), ("end_date", DT),
        ("duration_days", I), ("discount_percentage", DEC),
        ("campaign_cost", DEC), ("campaign_revenue", DEC), ("campaign_units", I),
        ("campaign_gross_profit", DEC), ("baseline_window_days", I),
        ("baseline_daily_revenue", D), ("baseline_daily_units", D),
        ("expected_baseline_revenue", DEC), ("expected_baseline_units", DEC),
        ("incremental_revenue", DEC), ("incremental_units", DEC),
        ("incremental_gross_profit", DEC), ("roi", D), ("roi_margin_based", D),
        ("uplift_pct", D), ("is_profitable", B),
    ],
}

# Power Query type names, keyed by TMSL type.
M_TYPE = {S: "type text", I: "Int64.Type", D: "type number",
          DEC: "type number", DT: "type datetime", B: "type logical"}

# --------------------------------------------------------------------------
# Relationships — all one-to-many, dimension → fact, single direction
# --------------------------------------------------------------------------
# mart_campaign_roi joins ONLY to dim_campaign. It also carries product_key, and
# joining that to dim_product would close a loop
# (dim_product → fact_sales → dim_campaign → mart → dim_product), which Power BI
# resolves by silently deactivating a relationship. The mart already carries
# product_name/category/brand as columns, so use those instead.

RELATIONSHIPS = [
    ("dim_date", "date_key", "fact_sales", "date_key"),
    ("dim_product", "product_key", "fact_sales", "product_key"),
    ("dim_store", "store_key", "fact_sales", "store_key"),
    ("dim_campaign", "campaign_key", "fact_sales", "campaign_key"),
    ("dim_date", "date_key", "fact_inventory_snapshot", "date_key"),
    ("dim_product", "product_key", "fact_inventory_snapshot", "product_key"),
    ("dim_store", "store_key", "fact_inventory_snapshot", "store_key"),
    ("dim_campaign", "campaign_key", "mart_campaign_roi", "campaign_key"),
]

# --------------------------------------------------------------------------
# Measures — ported from powerbi/measures.dax
# --------------------------------------------------------------------------
# (name, DAX, format string, display folder)

CURRENCY = '\\₹#,0;(\\₹#,0);\\₹#,0'
PCT = "0.0%"
NUM = "#,0"
DECIMAL2 = "0.00"

MEASURES: list[tuple[str, str, str, str]] = [
    # --- sales core ---
    ("Total Revenue", "SUM ( fact_sales[net_revenue] )", CURRENCY, "Sales"),
    ("Units Sold", "SUM ( fact_sales[quantity] )", NUM, "Sales"),
    ("Transactions", "DISTINCTCOUNT ( fact_sales[transaction_id] )", NUM, "Sales"),
    ("Total COGS", "SUM ( fact_sales[cogs] )", CURRENCY, "Sales"),
    ("Gross Profit", "SUM ( fact_sales[gross_profit] )", CURRENCY, "Sales"),
    ("Gross Margin %", "DIVIDE ( [Gross Profit], [Total Revenue] )", PCT, "Sales"),
    ("Gross Sales", "SUM ( fact_sales[gross_amount] )", CURRENCY, "Sales"),
    ("Total Discount", "SUM ( fact_sales[discount_amount] )", CURRENCY, "Sales"),
    ("Discount Rate %", "DIVIDE ( [Total Discount], [Gross Sales] )", PCT, "Sales"),
    ("Average Basket Value", "DIVIDE ( [Total Revenue], [Transactions] )", CURRENCY, "Sales"),
    ("Units per Transaction", "DIVIDE ( [Units Sold], [Transactions] )", DECIMAL2, "Sales"),

    # --- promotion split ---
    ("Promoted Revenue",
     "CALCULATE ( [Total Revenue], fact_sales[is_promoted] = TRUE () )", CURRENCY, "Promotion"),
    ("Non-Promoted Revenue",
     "CALCULATE ( [Total Revenue], fact_sales[is_promoted] = FALSE () )", CURRENCY, "Promotion"),
    ("Promoted Revenue %",
     "DIVIDE ( [Promoted Revenue], [Total Revenue] )", PCT, "Promotion"),
    ("Promoted Units",
     "CALCULATE ( [Units Sold], fact_sales[is_promoted] = TRUE () )", NUM, "Promotion"),

    # --- campaign ROI (from the mart) ---
    ("Campaign Cost", "SUM ( mart_campaign_roi[campaign_cost] )", CURRENCY, "Campaign ROI"),
    ("Campaign Revenue", "SUM ( mart_campaign_roi[campaign_revenue] )", CURRENCY, "Campaign ROI"),
    ("Incremental Revenue",
     "SUM ( mart_campaign_roi[incremental_revenue] )", CURRENCY, "Campaign ROI"),
    ("Incremental Units",
     "SUM ( mart_campaign_roi[incremental_units] )", NUM, "Campaign ROI"),
    ("Incremental Gross Profit",
     "SUM ( mart_campaign_roi[incremental_gross_profit] )", CURRENCY, "Campaign ROI"),
    # Recomputed from summed components, NOT an average of the roi column:
    # averaging ratios weights a tiny campaign the same as a large one and gives
    # a number that does not reconcile to the totals.
    ("Portfolio ROI",
     "DIVIDE ( [Incremental Revenue] - [Campaign Cost], [Campaign Cost] )", DECIMAL2, "Campaign ROI"),
    ("Portfolio ROI (Margin)",
     "DIVIDE ( [Incremental Gross Profit] - [Campaign Cost], [Campaign Cost] )",
     DECIMAL2, "Campaign ROI"),
    ("Average Campaign ROI", "AVERAGE ( mart_campaign_roi[roi] )", DECIMAL2, "Campaign ROI"),
    ("Campaign Count", "COUNTROWS ( mart_campaign_roi )", NUM, "Campaign ROI"),
    ("Profitable Campaigns",
     "COUNTROWS ( FILTER ( mart_campaign_roi, mart_campaign_roi[roi] > 0 ) )",
     NUM, "Campaign ROI"),
    ("Profitable Campaign %",
     "DIVIDE ( [Profitable Campaigns], [Campaign Count] )", PCT, "Campaign ROI"),
    ("Campaign Uplift %",
     "DIVIDE ( [Incremental Revenue], SUM ( mart_campaign_roi[expected_baseline_revenue] ) )",
     PCT, "Campaign ROI"),

    # --- inventory (semi-additive) ---
    # Stock may be summed across products and stores but NEVER across dates:
    # the same physical units appear in every weekly snapshot. Sum within a
    # snapshot date, average across dates.
    ("Avg Inventory Units",
     "AVERAGEX (\n"
     "    VALUES ( fact_inventory_snapshot[snapshot_date] ),\n"
     "    CALCULATE ( SUM ( fact_inventory_snapshot[stock_quantity] ) )\n"
     ")", NUM, "Inventory"),
    ("Avg Inventory Value",
     "AVERAGEX (\n"
     "    VALUES ( fact_inventory_snapshot[snapshot_date] ),\n"
     "    CALCULATE ( SUM ( fact_inventory_snapshot[stock_value_at_cost] ) )\n"
     ")", CURRENCY, "Inventory"),
    ("Closing Stock Units",
     "CALCULATE (\n"
     "    SUM ( fact_inventory_snapshot[stock_quantity] ),\n"
     "    LASTNONBLANK ( fact_inventory_snapshot[snapshot_date], 1 )\n"
     ")", NUM, "Inventory"),
    # COGS and stock BOTH valued at cost — mixing retail-valued stock with
    # cost-valued COGS would understate turns.
    ("Inventory Turnover",
     "DIVIDE ( [Total COGS], [Avg Inventory Value] )", DECIMAL2, "Inventory"),
    ("Days of Supply",
     "VAR DaysInContext = COUNTROWS ( VALUES ( dim_date[date] ) )\n"
     "RETURN\n"
     "    DIVIDE ( DaysInContext, [Inventory Turnover] )", DECIMAL2, "Inventory"),

    # --- time intelligence (needs dim_date marked as a date table) ---
    ("Revenue Prior Month",
     "CALCULATE ( [Total Revenue], DATEADD ( dim_date[date], -1, MONTH ) )",
     CURRENCY, "Time"),
    ("Revenue MoM %",
     "DIVIDE ( [Total Revenue] - [Revenue Prior Month], [Revenue Prior Month] )",
     PCT, "Time"),
    ("Revenue YTD", "TOTALYTD ( [Total Revenue], dim_date[date] )", CURRENCY, "Time"),

    # --- store performance ---
    ("Store Rank by Revenue",
     "RANKX ( ALLSELECTED ( dim_store[store_name] ), [Total Revenue],, DESC, DENSE )",
     NUM, "Store"),
    ("Revenue per Store",
     "DIVIDE ( [Total Revenue], DISTINCTCOUNT ( dim_store[store_key] ) )",
     CURRENCY, "Store"),
    ("Revenue % of Total",
     "DIVIDE ( [Total Revenue], CALCULATE ( [Total Revenue], ALLSELECTED ( dim_store ) ) )",
     PCT, "Store"),

    # --- data quality, surfaced on the report on purpose ---
    ("Unattributed Campaign Rows",
     "CALCULATE ( COUNTROWS ( fact_sales ), fact_sales[campaign_resolved] = FALSE () )",
     NUM, "Data Quality"),
    ("Campaign Attribution %",
     "DIVIDE (\n"
     "    CALCULATE ( COUNTROWS ( fact_sales ), fact_sales[campaign_resolved] = TRUE () ),\n"
     "    COUNTROWS ( fact_sales )\n"
     ")", PCT, "Data Quality"),
]

PAGES = ["Executive Overview", "Campaign Analysis", "Store Performance", "Inventory"]


# --------------------------------------------------------------------------
# Builders
# --------------------------------------------------------------------------

def m_expression(table: str, columns: list[tuple[str, str]]) -> list[str]:
    """Power Query that reads the exported CSV and applies explicit types.

    Types are set explicitly rather than left to Power BI's inference: inference
    reads only the first 200 rows, and would type `campaign_key` as a whole
    number in one table and text in another, silently breaking the relationship.
    """
    transforms = ", ".join(f'{{"{c}", {M_TYPE[t]}}}' for c, t in columns)
    return [
        "let",
        f'    Source = Csv.Document(File.Contents(DataFolder & "{table}.csv"), '
        "[Delimiter=\",\", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),",
        "    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),",
        f"    Typed = Table.TransformColumnTypes(Headers, {{{transforms}}})",
        "in",
        "    Typed",
    ]


def build_model() -> dict:
    tables = []
    for name, cols in TABLES.items():
        table: dict = {
            "name": name,
            "columns": [
                {
                    "name": c,
                    "dataType": t,
                    "sourceColumn": c,
                    # Surrogate keys are join plumbing, not something to slice by.
                    **({"isHidden": True} if c.endswith("_key") and name.startswith("fact") else {}),
                }
                for c, t in cols
            ],
            "partitions": [{
                "name": f"{name}-partition",
                "mode": "import",
                "source": {"type": "m", "expression": m_expression(name, cols)},
            }],
        }
        if name == "dim_date":
            # Marks this as the model's date table. Without it, DATEADD and
            # TOTALYTD return wrong answers *silently* rather than erroring.
            table["dataCategory"] = "Time"
            for col in table["columns"]:
                if col["name"] == "date":
                    col["isKey"] = True
        tables.append(table)

    # Measures live on a dedicated table so they group together in the field
    # list instead of hiding inside whichever table they happen to reference.
    tables.append({
        "name": "_Measures",
        "columns": [{"name": "_placeholder", "dataType": "string",
                     "sourceColumn": "_placeholder", "isHidden": True}],
        "partitions": [{
            "name": "_Measures-partition",
            "mode": "import",
            "source": {"type": "m", "expression": [
                "let",
                '    Source = #table({"_placeholder"}, {{""}})',
                "in",
                "    Source",
            ]},
        }],
        "measures": [
            {
                "name": n,
                "expression": dax.split("\n") if "\n" in dax else dax,
                "formatString": fmt,
                "displayFolder": folder,
            }
            for n, dax, fmt, folder in MEASURES
        ],
    })

    relationships = [
        {
            "name": f"rel_{ft}_{fc}_{tt}",
            "fromTable": ft_tbl,
            "fromColumn": ft_col,
            "toTable": t_tbl,
            "toColumn": t_col,
        }
        for (t_tbl, t_col, ft_tbl, ft_col) in RELATIONSHIPS
        for ft, fc, tt in [(ft_tbl, ft_col, t_tbl)]
    ]

    return {
        "name": "RetailIntel",
        "compatibilityLevel": 1567,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {
                "legacyRedirects": True,
                "returnErrorValuesAsNull": True,
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "expressions": [{
                "name": "DataFolder",
                "kind": "m",
                # The one thing that must be edited after opening.
                "expression": [
                    '"C:\\RetailIntel\\data\\" meta '
                    '[IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'
                ],
            }],
            "tables": tables,
            "relationships": relationships,
            "annotations": [
                {"name": "PBI_QueryOrder",
                 "value": json.dumps(list(TABLES) + ["_Measures"])},
            ],
        },
    }


def build_report() -> dict:
    """Four named pages, no visuals — see the module docstring for why."""
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "config": json.dumps({
            "version": "5.55",
            "themeCollection": {"baseTheme": {"name": "CY24SU10"}},
            "activeSectionIndex": 0,
            "defaultDrillFilterOtherVisuals": True,
        }),
        "layoutOptimization": 0,
        "resourcePackages": [{
            "resourcePackage": {
                "disabled": False, "items": [
                    {"name": "CY24SU10", "path": "BaseThemes/CY24SU10.json", "type": 202}
                ],
                "name": "SharedResources", "type": 2,
            }
        }],
        "sections": [
            {
                "name": f"page{i}",
                "displayName": title,
                "ordinal": i,
                "width": 1280,
                "height": 720,
                "displayOption": 1,
                "config": json.dumps({}),
                "filters": "[]",
                "visualContainers": [],
            }
            for i, title in enumerate(PAGES)
        ],
    }


def main() -> None:
    for d in (MODEL_DIR, REPORT_DIR):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    # --- project pointer -------------------------------------------------
    (PBI / "RetailIntel.pbip").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/pbip/definition/1.0.0/schema.json",
        "version": "1.0",
        "artifacts": [{"report": {"path": "RetailIntel.Report"}}],
        "settings": {"enableAutoRecovery": True},
    }, indent=2))

    # --- semantic model --------------------------------------------------
    (MODEL_DIR / "definition.pbism").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definition/1.0.0/schema.json",
        "version": "4.2",
        "settings": {},
    }, indent=2))
    (MODEL_DIR / "model.bim").write_text(json.dumps(build_model(), indent=2))

    # --- report ----------------------------------------------------------
    (REPORT_DIR / "definition.pbir").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/1.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byPath": {"path": "../RetailIntel.SemanticModel"},
            "byConnection": None,
        },
    }, indent=2))
    (REPORT_DIR / "report.json").write_text(json.dumps(build_report(), indent=2))

    n_cols = sum(len(c) for c in TABLES.values())
    print("PBIP project written to powerbi/")
    print(f"  tables        : {len(TABLES)} (+ _Measures)")
    print(f"  columns typed : {n_cols}")
    print(f"  relationships : {len(RELATIONSHIPS)}")
    print(f"  measures      : {len(MEASURES)}")
    print(f"  report pages  : {len(PAGES)} (no visuals — place them in Desktop)")


if __name__ == "__main__":
    main()
