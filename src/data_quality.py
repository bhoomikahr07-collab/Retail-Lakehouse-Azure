"""
Automated data quality checks used as a gate between bronze and silver.
"""
import logging
import os

from pyspark.sql import functions as F


def get_logger(log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("retail_lakehouse")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def tag_null_check(df, not_null_columns, reason_col="_dq_failure_reason"):
    condition = None
    for col in not_null_columns:
        col_condition = F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")
        condition = col_condition if condition is None else (condition | col_condition)

    return df.withColumn(
        reason_col,
        F.when(condition, F.concat_ws(
            ",", F.coalesce(F.col(reason_col), F.lit("")), F.lit("null_required_field")
        )).otherwise(F.col(reason_col)),
    )


def tag_numeric_check(df, numeric_columns, reason_col="_dq_failure_reason"):
    condition = None
    for col in numeric_columns:
        col_condition = F.col(col).cast("double").isNull() & F.col(col).isNotNull()
        condition = col_condition if condition is None else (condition | col_condition)

    return df.withColumn(
        reason_col,
        F.when(condition, F.concat_ws(
            ",", F.coalesce(F.col(reason_col), F.lit("")), F.lit("non_numeric_value")
        )).otherwise(F.col(reason_col)),
    )


def tag_duplicate_check(df, unique_columns, reason_col="_dq_failure_reason"):
    from pyspark.sql.window import Window

    w = Window.partitionBy(*unique_columns)
    df = df.withColumn("_dq_dup_count", F.count("*").over(w))
    df = df.withColumn(
        reason_col,
        F.when(F.col("_dq_dup_count") > 1, F.concat_ws(
            ",", F.coalesce(F.col(reason_col), F.lit("")), F.lit("duplicate_key")
        )).otherwise(F.col(reason_col)),
    )
    return df.drop("_dq_dup_count")


def run_dq_checks(df, rules: dict, logger: logging.Logger):
    reason_col = "_dq_failure_reason"
    checked = df.withColumn(reason_col, F.lit(None).cast("string"))
    checked = tag_null_check(checked, rules.get("not_null_columns", []), reason_col)
    checked = tag_numeric_check(checked, rules.get("numeric_columns", []), reason_col)
    checked = tag_duplicate_check(checked, rules.get("unique_columns", []), reason_col)

    total = checked.count()
    quarantined = checked.filter(F.col(reason_col).isNotNull())
    clean = checked.filter(F.col(reason_col).isNull()).drop(reason_col)

    bad_count = quarantined.count()
    pct_bad = (bad_count / total * 100) if total else 0.0
    logger.info(f"DQ check complete: {total} rows checked, {bad_count} quarantined ({pct_bad:.2f}%)")
    if pct_bad > rules.get("max_quarantine_pct", 5.0):
        logger.warning(
            f"Quarantine rate {pct_bad:.2f}% exceeds threshold "
            f"{rules.get('max_quarantine_pct', 5.0)}% — investigate source data."
        )

    return clean, quarantined
