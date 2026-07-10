"""Silver layer: bronze to cleaned, conformed, DQ-gated Delta tables."""
import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from data_quality import get_logger, run_dq_checks
from utils import get_spark_session, load_config, load_dq_rules


def clean_and_cast_transactions(clean_df):
    return (
        clean_df
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .dropDuplicates(["transaction_id"])
    )


def conform_reference_table(df, key_col):
    return df.dropDuplicates([key_col]).filter(F.col(key_col).isNotNull())


def main(env: str):
    cfg = load_config(env)
    rules = load_dq_rules()
    logger = get_logger(cfg["log_path"])
    spark = get_spark_session("retail-lakehouse-silver")

    logger.info(f"=== Silver transform run started (env={env}) ===")
    transactions_bronze = spark.read.format("delta").load(os.path.join(cfg["bronze_path"], "transactions"))

    clean_txns, quarantined_txns = run_dq_checks(transactions_bronze, rules, logger)
    silver_txns = clean_and_cast_transactions(clean_txns)

    (
        silver_txns.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(os.path.join(cfg["silver_path"], "transactions"))
    )
    (
        quarantined_txns.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(os.path.join(cfg["quarantine_path"], "transactions"))
    )
    logger.info(
        f"[silver] transactions: {silver_txns.count()} rows written to silver, "
        f"{quarantined_txns.count()} rows quarantined"
    )

    reference_tables = [
        ("products", "product_id"),
        ("categories", "category_id"),
        ("departments", "department_id"),
        ("stores", "store_id"),
        ("regions", "region_id"),
        ("suppliers", "supplier_id"),
        ("supply_chain", "shipment_id"),
    ]
    for table_name, key_col in reference_tables:
        bronze_df = spark.read.format("delta").load(os.path.join(cfg["bronze_path"], table_name))
        silver_df = conform_reference_table(bronze_df, key_col)
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            os.path.join(cfg["silver_path"], table_name)
        )
        logger.info(f"[silver] {table_name}: {silver_df.count()} rows written to silver")

    logger.info("=== Silver transform run complete ===")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["local", "azure"], default="local")
    args = parser.parse_args()
    main(args.env)
