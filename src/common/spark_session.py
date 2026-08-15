"""Spark session factory.

Locally we start a Delta-enabled Spark in local[*] mode. On Databricks a session
already exists and the runtime ships Delta, so we just grab the active one. Every
module calls get_spark() rather than building its own, which keeps configuration
in exactly one place.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.project_config import ENV, SPARK  # noqa: E402

_session = None


def get_spark(app_name: str | None = None):
    """Return a Delta-enabled SparkSession (cached per process)."""
    global _session
    if _session is not None:
        return _session

    from pyspark.sql import SparkSession

    if ENV == "databricks":
        # The cluster already provides a session with Delta configured.
        _session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        _session.conf.set("spark.sql.shuffle.partitions", str(SPARK.shuffle_partitions))
        return _session

    # ---- local -----------------------------------------------------------
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        for candidate in (
            "/opt/homebrew/opt/openjdk@17",
            "/usr/local/opt/openjdk@17",
            "/opt/homebrew/opt/openjdk@11",
        ):
            if Path(candidate).exists():
                os.environ["JAVA_HOME"] = candidate
                break

    # On macOS the machine hostname often does not resolve, and Spark's driver
    # then fails to bind with UnresolvedAddressException. Loopback is correct
    # for a single-process local[*] run and avoids depending on DNS.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    # Spark launches Python workers via PYSPARK_PYTHON, which defaults to
    # whatever `python3` resolves to on PATH — not necessarily the interpreter
    # running the driver. A minor-version mismatch aborts the job, so pin both
    # ends to this interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name or SPARK.app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # 132K rows does not need 200 shuffle partitions; 200 tiny files would
        # cost more in task overhead than the shuffle itself saves.
        .config("spark.sql.shuffle.partitions", str(SPARK.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # Keeps the local run quiet and avoids a Derby metastore in the repo root.
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
    )
    _session = configure_spark_with_delta_pip(builder).getOrCreate()
    _session.sparkContext.setLogLevel("ERROR")
    return _session


def stop_spark() -> None:
    global _session
    if _session is not None:
        _session.stop()
        _session = None
