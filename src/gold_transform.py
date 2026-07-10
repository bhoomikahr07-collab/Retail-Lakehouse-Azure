"""Gold layer: silver to analytics-ready snowflake schema tables."""
import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from utils import get_spark_session, load_config


def read_silver(spark, silver_path, table_name):
    return spark.read.format("delta").load(os.path.join(silver_path, table_name))


def build_fact_sales(transactions):
    return (
        transactions
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("date_key", F.to_date("transaction_ts"))
        .select(
            "transaction_id", "transaction_ts", "date_key", "store_id", "product_id",
            "quantity", "unit_price", "revenue", "payment_type",
        )
    )


def build_dim_supply_chain_summary(supply_chain):
    return (
        supply_chain
        .groupBy("product_id", "supplier_id")
        .agg(
            F.sum("quantity_received").alias("total_quantity_received"),
            F.count("*").alias("shipment_count"),
            F.max("ship_date").alias("last_shipment_date"),
        )
    )


def build_daily_summary(fact_sales, products, categories, stores, regions):
    enriched = (
        fact_sales
        .join(products.select("product_id", "category_id"), "product_id", "left")
        .join(categories.select("category_id", "category_name", "department_id"), "category_id", "left")
        .join(stores.select("store_id", "region_id"), "store_id", "left")
        .join(regions.select("region_id", "region_name"), "region_id", "left")
    )
    return (
        enriched
        .groupBy("date_key", "region_name", "category_name")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("transaction_id").alias("transaction_count"),
        )
        .orderBy("date_key")
    )


def write_gold(df, gold_path, table_name, partition_col=None):
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    out_path = os.path.join(gold_path, table_name)
    writer.save(out_path)
    df.coalesce(1).write.mode("overwrite").parquet(os.path.join(gold_path, f"{table_name}_parquet"))
    print(f"[gold] {table_name}: wrote {df.count()} rows -> {out_path}")


def main(env: str):
    cfg = load_config(env)
    spark = get_spark_session("retail-lakehouse-gold")

    silver_path = cfg["silver_path"]
    transactions = read_silver(spark, silver_path, "transactions")
    products = read_silver(spark, silver_path, "products")
    categories = read_silver(spark, silver_path, "categories")
    departments = read_silver(spark, silver_path, "departments")
    stores = read_silver(spark, silver_path, "stores")
    regions = read_silver(spark, silver_path, "regions")
    suppliers = read_silver(spark, silver_path, "suppliers")
    supply_chain = read_silver(spark, silver_path, "supply_chain")

    fact_sales = build_fact_sales(transactions)
    supply_summary = build_dim_supply_chain_summary(supply_chain)
    daily_summary = build_daily_summary(fact_sales, products, categories, stores, regions)

    gold_path = cfg["gold_path"]
    write_gold(fact_sales, gold_path, "fact_sales", partition_col="date_key")
    write_gold(products, gold_path, "dim_product")
    write_gold(categories, gold_path, "dim_category")
    write_gold(departments, gold_path, "dim_department")
    write_gold(stores, gold_path, "dim_store")
    write_gold(regions, gold_path, "dim_region")
    write_gold(suppliers, gold_path, "dim_supplier")
    write_gold(supply_summary, gold_path, "fact_supply_chain_summary")
    write_gold(daily_summary, gold_path, "gold_daily_merchandising_summary")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["local", "azure"], default="local")
    args = parser.parse_args()
    main(args.env)
