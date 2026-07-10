"""Bronze layer: raw CSV to append-only Delta tables with ingestion metadata."""
import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from schema import (
    TRANSACTIONS_SCHEMA, PRODUCTS_SCHEMA, CATEGORIES_SCHEMA, DEPARTMENTS_SCHEMA,
    STORES_SCHEMA, REGIONS_SCHEMA, SUPPLIERS_SCHEMA, SUPPLY_CHAIN_SCHEMA,
)
from utils import get_spark_session, load_config


def ingest_source(spark, raw_path, filename, schema, table_name, bronze_path):
    df = (
        spark.read.option("header", True).schema(schema)
        .csv(os.path.join(raw_path, filename))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )
    out_path = os.path.join(bronze_path, table_name)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(out_path)
    )
    print(f"[bronze] {table_name}: appended {df.count()} rows -> {out_path}")


def main(env: str):
    cfg = load_config(env)
    spark = get_spark_session("retail-lakehouse-bronze")

    sources = [
        ("transactions.csv", TRANSACTIONS_SCHEMA, "transactions"),
        ("products.csv", PRODUCTS_SCHEMA, "products"),
        ("categories.csv", CATEGORIES_SCHEMA, "categories"),
        ("departments.csv", DEPARTMENTS_SCHEMA, "departments"),
        ("stores.csv", STORES_SCHEMA, "stores"),
        ("regions.csv", REGIONS_SCHEMA, "regions"),
        ("suppliers.csv", SUPPLIERS_SCHEMA, "suppliers"),
        ("supply_chain.csv", SUPPLY_CHAIN_SCHEMA, "supply_chain"),
    ]

    for filename, schema, table_name in sources:
        ingest_source(spark, cfg["raw_path"], filename, schema, table_name, cfg["bronze_path"])

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["local", "azure"], default="local")
    args = parser.parse_args()
    main(args.env)
