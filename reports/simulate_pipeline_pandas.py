"""
simulate_pipeline_pandas.py

Pandas reimplementation of the bronze/silver/gold and data-quality logic
for environments without a Spark/Delta runtime.
"""
import os

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

NOT_NULL_COLUMNS = ["store_id"]
NUMERIC_COLUMNS = ["quantity"]
UNIQUE_COLUMNS = ["transaction_id"]
MAX_QUARANTINE_PCT = 5.0


def run_dq_checks(df):
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)

    for col in NOT_NULL_COLUMNS:
        is_null = df[col].isna() | (df[col].astype(str).str.strip() == "")
        for idx in df.index[is_null]:
            reasons[idx].append("null_required_field")

    for col in NUMERIC_COLUMNS:
        numeric = pd.to_numeric(df[col], errors="coerce")
        is_bad = numeric.isna() & df[col].notna()
        for idx in df.index[is_bad]:
            reasons[idx].append("non_numeric_value")

    for col in UNIQUE_COLUMNS:
        dup_mask = df.duplicated(subset=[col], keep=False)
        for idx in df.index[dup_mask]:
            reasons[idx].append("duplicate_key")

    df = df.copy()
    df["_dq_failure_reason"] = reasons.apply(lambda r: ",".join(r) if r else None)
    quarantined = df[df["_dq_failure_reason"].notna()]
    clean = df[df["_dq_failure_reason"].isna()].drop(columns=["_dq_failure_reason"])
    return clean, quarantined


def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    lines = []

    def log(msg=""):
        print(msg)
        lines.append(msg)

    log("# Pipeline Run Report (local pandas simulation)\n")
    log("Mirrors src/data_quality.py + src/silver_transform.py + src/gold_transform.py logic, run locally against the sample data in data/raw/.\n")

    transactions_raw = pd.read_csv(os.path.join(RAW_DIR, "transactions.csv"), keep_default_na=False, na_values=[""])
    products = pd.read_csv(os.path.join(RAW_DIR, "products.csv"))
    categories = pd.read_csv(os.path.join(RAW_DIR, "categories.csv"))
    departments = pd.read_csv(os.path.join(RAW_DIR, "departments.csv"))
    stores = pd.read_csv(os.path.join(RAW_DIR, "stores.csv"))
    regions = pd.read_csv(os.path.join(RAW_DIR, "regions.csv"))
    suppliers = pd.read_csv(os.path.join(RAW_DIR, "suppliers.csv"))
    supply_chain = pd.read_csv(os.path.join(RAW_DIR, "supply_chain.csv"))

    log("## 1. Bronze — raw ingest\n")
    log(f"- transactions.csv: {len(transactions_raw)} rows")
    log(f"- products.csv: {len(products)} rows, categories.csv: {len(categories)} rows, departments.csv: {len(departments)} rows")
    log(f"- stores.csv: {len(stores)} rows, regions.csv: {len(regions)} rows")
    log(f"- suppliers.csv: {len(suppliers)} rows, supply_chain.csv: {len(supply_chain)} rows\n")

    clean_txns, quarantined_txns = run_dq_checks(transactions_raw)
    pct_quarantined = len(quarantined_txns) / len(transactions_raw) * 100

    log("## 2. Silver — data quality gate\n")
    log(f"- rows checked: {len(transactions_raw)}")
    log(f"- rows quarantined: {len(quarantined_txns)} ({pct_quarantined:.2f}%)")
    log(f"- rows passed to silver: {len(clean_txns)}")
    status = "OK" if pct_quarantined <= MAX_QUARANTINE_PCT else f"ALERT (> {MAX_QUARANTINE_PCT}% threshold)"
    log(f"- quarantine rate status: {status}\n")

    log("**Quarantined rows (failure reasons):**\n")
    log("| transaction_id | store_id | quantity | unit_price | failure_reason |")
    log("|---|---|---|---|---|")
    for _, r in quarantined_txns.iterrows():
        log(f"| {r['transaction_id']} | {r['store_id']} | {r['quantity']} | {r['unit_price']} | {r['_dq_failure_reason']} |")
    log("")

    clean_txns = clean_txns.copy()
    clean_txns["quantity"] = clean_txns["quantity"].astype(int)
    clean_txns["unit_price"] = clean_txns["unit_price"].astype(float)
    silver_txns = clean_txns.drop_duplicates(subset=["transaction_id"])
    log(f"- silver transactions after dedupe: {len(silver_txns)} rows\n")

    fact_sales = silver_txns.copy()
    fact_sales["revenue"] = (fact_sales["quantity"] * fact_sales["unit_price"]).round(2)
    fact_sales["date_key"] = pd.to_datetime(fact_sales["transaction_ts"]).dt.date

    log("## 3. Gold — analytics-ready tables\n")
    log(f"- fact_sales: {len(fact_sales)} rows, ${fact_sales['revenue'].sum():,.2f} total revenue")
    log(f"- dim_product: {products['product_id'].nunique()} rows")
    log(f"- dim_category: {categories['category_id'].nunique()} rows")
    log(f"- dim_department: {departments['department_id'].nunique()} rows")
    log(f"- dim_store: {stores['store_id'].nunique()} rows")
    log(f"- dim_region: {regions['region_id'].nunique()} rows")
    log(f"- dim_supplier: {suppliers['supplier_id'].nunique()} rows\n")

    enriched = (
        fact_sales
        .merge(products[["product_id", "category_id"]], on="product_id", how="left")
        .merge(categories[["category_id", "category_name"]], on="category_id", how="left")
        .merge(stores[["store_id", "region_id"]], on="store_id", how="left")
        .merge(regions[["region_id", "region_name"]], on="region_id", how="left")
    )
    daily_summary = (
        enriched.groupby(["region_name", "category_name"])
        .agg(total_revenue=("revenue", "sum"), total_units=("quantity", "sum"), transaction_count=("transaction_id", "nunique"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    log("## 4. Sample gold_daily_merchandising_summary output (top 8 by revenue)\n")
    log("| region | category | total_revenue | total_units | transactions |")
    log("|---|---|---|---|---|")
    for _, r in daily_summary.head(8).iterrows():
        log(f"| {r['region_name']} | {r['category_name']} | ${r['total_revenue']:,.2f} | {r['total_units']} | {r['transaction_count']} |")
    log("")

    supply_summary = (
        supply_chain.groupby(["product_id", "supplier_id"])
        .agg(total_quantity_received=("quantity_received", "sum"), shipment_count=("shipment_id", "count"))
        .reset_index()
        .sort_values("total_quantity_received", ascending=False)
    )
    log("## 5. Sample fact_supply_chain_summary output (top 5 by quantity received)\n")
    log("| product_id | supplier_id | total_quantity_received | shipment_count |")
    log("|---|---|---|---|")
    for _, r in supply_summary.head(5).iterrows():
        log(f"| {r['product_id']} | {r['supplier_id']} | {r['total_quantity_received']} | {r['shipment_count']} |")
    log("")

    report_path = os.path.join(REPORTS_DIR, "pipeline_run_report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nReport written to {report_path}")


if __name__ == "__main__":
    main()
