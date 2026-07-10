-- Load gold-layer Parquet output from ADLS Gen2 into Synapse.

TRUNCATE TABLE merchandising.dim_department;
COPY INTO merchandising.dim_department
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_department_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.dim_category;
COPY INTO merchandising.dim_category
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_category_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.dim_product;
COPY INTO merchandising.dim_product
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_product_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.dim_region;
COPY INTO merchandising.dim_region
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_region_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.dim_store;
COPY INTO merchandising.dim_store
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_store_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.dim_supplier;
COPY INTO merchandising.dim_supplier
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/dim_supplier_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE staging_fact_sales;
COPY INTO staging_fact_sales
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/fact_sales_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

MERGE merchandising.fact_sales AS target
USING staging_fact_sales AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET
    quantity = source.quantity,
    unit_price = source.unit_price,
    revenue = source.revenue
WHEN NOT MATCHED THEN
    INSERT (transaction_id, transaction_ts, date_key, store_id, product_id, quantity, unit_price, revenue, payment_type)
    VALUES (source.transaction_id, source.transaction_ts, source.date_key, source.store_id, source.product_id,
            source.quantity, source.unit_price, source.revenue, source.payment_type);

TRUNCATE TABLE merchandising.fact_supply_chain_summary;
COPY INTO merchandising.fact_supply_chain_summary
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/fact_supply_chain_summary_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

TRUNCATE TABLE merchandising.gold_daily_merchandising_summary;
COPY INTO merchandising.gold_daily_merchandising_summary
FROM 'https://retaillakehousedl.dfs.core.windows.net/lakehouse/gold/gold_daily_merchandising_summary_parquet/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));
