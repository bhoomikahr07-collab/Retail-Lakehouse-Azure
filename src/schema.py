"""Explicit schemas for every raw source file."""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType,
)

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ts", TimestampType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", StringType(), nullable=True),
    StructField("unit_price", StringType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category_id", StringType(), nullable=True),
    StructField("supplier_id", StringType(), nullable=True),
    StructField("unit_cost", DoubleType(), nullable=True),
])

CATEGORIES_SCHEMA = StructType([
    StructField("category_id", StringType(), nullable=False),
    StructField("category_name", StringType(), nullable=False),
    StructField("department_id", StringType(), nullable=True),
])

DEPARTMENTS_SCHEMA = StructType([
    StructField("department_id", StringType(), nullable=False),
    StructField("department_name", StringType(), nullable=False),
])

STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), nullable=False),
    StructField("store_name", StringType(), nullable=False),
    StructField("region_id", StringType(), nullable=True),
])

REGIONS_SCHEMA = StructType([
    StructField("region_id", StringType(), nullable=False),
    StructField("region_name", StringType(), nullable=False),
])

SUPPLIERS_SCHEMA = StructType([
    StructField("supplier_id", StringType(), nullable=False),
    StructField("supplier_name", StringType(), nullable=False),
    StructField("country", StringType(), nullable=True),
])

SUPPLY_CHAIN_SCHEMA = StructType([
    StructField("shipment_id", StringType(), nullable=False),
    StructField("ship_date", DateType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("supplier_id", StringType(), nullable=False),
    StructField("quantity_received", IntegerType(), nullable=False),
])
