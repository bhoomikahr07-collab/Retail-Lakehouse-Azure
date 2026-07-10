"""Unit tests for silver/gold transform logic."""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from data_quality import run_dq_checks
from gold_transform import build_fact_sales


@pytest.fixture(scope="module")
def spark():
    from utils import get_spark_session
    spark = get_spark_session("pytest-retail-lakehouse")
    yield spark
    spark.stop()


def spark_to_timestamp(col_name):
    from pyspark.sql import functions as F
    return F.to_timestamp(F.col(col_name))


def test_dq_gate_quarantines_bad_rows(spark):
    rows = [
        ("T1", "2026-06-01 10:00:00", "S1", "P1", "2", "10.0", "card"),
        ("T2", "2026-06-01 10:05:00", None, "P1", "1", "10.0", "cash"),
        ("T3", "2026-06-01 10:10:00", "S1", "P1", "N/A", "10.0", "cash"),
        ("T1", "2026-06-01 10:15:00", "S1", "P1", "2", "10.0", "card"),
    ]
    cols = ["transaction_id", "transaction_ts", "store_id", "product_id", "quantity", "unit_price", "payment_type"]
    df = spark.createDataFrame(rows, cols).withColumn("transaction_ts", spark_to_timestamp("transaction_ts"))

    rules = {
        "not_null_columns": ["store_id"],
        "numeric_columns": ["quantity"],
        "unique_columns": ["transaction_id"],
        "max_quarantine_pct": 100.0,
    }

    import logging
    logger = logging.getLogger("test")
    logger.addHandler(logging.NullHandler())

    clean, quarantined = run_dq_checks(df, rules, logger)
    assert quarantined.count() == 4
    assert clean.count() == 0


def test_build_fact_sales_computes_revenue_and_date_key(spark):
    from pyspark.sql import functions as F

    rows = [("T1", "2026-06-01 10:00:00", "S1", "P1", 3, 10.0, "card")]
    cols = ["transaction_id", "transaction_ts", "store_id", "product_id", "quantity", "unit_price", "payment_type"]
    df = spark.createDataFrame(rows, cols).withColumn("transaction_ts", F.to_timestamp("transaction_ts"))

    result = build_fact_sales(df).filter("transaction_id = 'T1'").collect()[0]
    assert result["revenue"] == 30.0
    assert str(result["date_key"]) == "2026-06-01"
