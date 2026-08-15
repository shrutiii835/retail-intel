"""Shared pytest fixtures.

One Spark session is built for the whole test session — starting a JVM per test
would make the suite unusably slow. Tests that write Delta use tmp_path, so they
never touch the real lakehouse.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from src.common.spark_session import get_spark, stop_spark

    s = get_spark("RetailIntelTests")
    yield s
    stop_spark()


@pytest.fixture(scope="session")
def source_dir() -> Path:
    from config.project_config import SOURCE_DIR

    p = Path(SOURCE_DIR)
    if not (p / "sales.csv").exists():
        pytest.skip(
            "generated source data not found — run "
            "`python -m src.data_generation.generate_data` first"
        )
    return p


@pytest.fixture(scope="session")
def generated(source_dir):
    """The generated CSVs, loaded once with pandas (all columns as strings)."""
    import pandas as pd

    return {
        name: pd.read_csv(source_dir / f"{name}.csv", dtype=str)
        for name in ("sales", "inventory", "campaigns", "products", "stores")
    }
