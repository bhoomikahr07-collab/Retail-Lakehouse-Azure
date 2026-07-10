# Pipeline Run Report (local pandas simulation)

Mirrors src/data_quality.py + src/silver_transform.py + src/gold_transform.py logic, run locally against the sample data in data/raw/ (no Spark/Delta runtime available in this environment — see reports/README.md).

## 1. Bronze — raw ingest

- transactions.csv: 704 rows
- products.csv: 30 rows, categories.csv: 9 rows, departments.csv: 4 rows
- stores.csv: 10 rows, regions.csv: 4 rows
- suppliers.csv: 4 rows, supply_chain.csv: 200 rows

## 2. Silver — data quality gate

- rows checked: 704
- rows quarantined: 4 (0.57%)
- rows passed to silver: 700
- quarantine rate status: OK

**Quarantined rows (failure reasons):**

| transaction_id | store_id | quantity | unit_price | failure_reason |
|---|---|---|---|---|
| TXN0000001 | S008 | 2 | 83.61 | duplicate_key |
| TXN0000001 | S001 | 2 | 19.99 | duplicate_key |
| TXN9999901 | nan | 1 | 9.99 | null_required_field |
| TXN9999902 | S001 | N/A | 9.99 | non_numeric_value |

- silver transactions after dedupe: 700 rows

## 3. Gold — analytics-ready tables

- fact_sales: 700 rows, $221,587.66 total revenue
- dim_product: 30 rows
- dim_category: 9 rows
- dim_department: 4 rows
- dim_store: 10 rows
- dim_region: 4 rows
- dim_supplier: 4 rows

## 4. Sample gold_daily_merchandising_summary output (top 8 by revenue)

| region | category | total_revenue | total_units | transactions |
|---|---|---|---|---|
| Midwest | Women's Apparel | $22,739.25 | 229 | 66 |
| Midwest | Packaged Foods | $18,444.91 | 161 | 46 |
| Northeast | Women's Apparel | $12,023.28 | 128 | 39 |
| South | Women's Apparel | $10,210.31 | 101 | 33 |
| West | Women's Apparel | $9,727.86 | 128 | 42 |
| Midwest | Beverages | $9,526.07 | 129 | 39 |
| Midwest | Furniture | $9,172.78 | 85 | 28 |
| Midwest | Kitchenware | $8,402.63 | 78 | 24 |

## 5. Sample fact_supply_chain_summary output (top 5 by quantity received)

| product_id | supplier_id | total_quantity_received | shipment_count |
|---|---|---|---|
| P0023 | SUP3 | 1758 | 5 |
| P0007 | SUP3 | 1518 | 4 |
| P0026 | SUP3 | 1429 | 4 |
| P0006 | SUP4 | 1325 | 4 |
| P0023 | SUP1 | 1211 | 4 |
