-- Snowflake schema for merchandising and supply-chain reporting.
-- Target: Azure Synapse Analytics dedicated SQL pool.

CREATE SCHEMA merchandising;
GO

CREATE TABLE merchandising.dim_department (
    department_id VARCHAR(10) NOT NULL,
    department_name VARCHAR(100) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.dim_category (
    category_id VARCHAR(10) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    department_id VARCHAR(10) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.dim_product (
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category_id VARCHAR(10),
    supplier_id VARCHAR(10),
    unit_cost DECIMAL(10,2)
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.dim_region (
    region_id VARCHAR(10) NOT NULL,
    region_name VARCHAR(50) NOT NULL
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.dim_store (
    store_id VARCHAR(20) NOT NULL,
    store_name VARCHAR(200) NOT NULL,
    region_id VARCHAR(10)
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.dim_supplier (
    supplier_id VARCHAR(10) NOT NULL,
    supplier_name VARCHAR(200) NOT NULL,
    country VARCHAR(50)
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.fact_sales (
    transaction_id VARCHAR(20) NOT NULL,
    transaction_ts DATETIME2 NOT NULL,
    date_key DATE NOT NULL,
    store_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    revenue DECIMAL(12,2) NOT NULL,
    payment_type VARCHAR(20)
)
WITH (DISTRIBUTION = HASH(product_id), CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.fact_supply_chain_summary (
    product_id VARCHAR(20) NOT NULL,
    supplier_id VARCHAR(10) NOT NULL,
    total_quantity_received INT NOT NULL,
    shipment_count INT NOT NULL,
    last_shipment_date DATE
)
WITH (DISTRIBUTION = HASH(product_id), CLUSTERED COLUMNSTORE INDEX);
GO

CREATE TABLE merchandising.gold_daily_merchandising_summary (
    date_key DATE NOT NULL,
    region_name VARCHAR(50),
    category_name VARCHAR(100),
    total_revenue DECIMAL(14,2),
    total_units INT,
    transaction_count INT
)
WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX);
GO
