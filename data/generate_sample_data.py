"""Generate local retail sample data, including planted DQ issues."""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(7)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")

DEPARTMENTS = [("D1", "Apparel & Accessories"), ("D2", "Home & Living"), ("D3", "Electronics"), ("D4", "Grocery")]
CATEGORIES = [
    ("C1", "Men's Apparel", "D1"), ("C2", "Women's Apparel", "D1"), ("C3", "Footwear", "D1"),
    ("C4", "Furniture", "D2"), ("C5", "Kitchenware", "D2"),
    ("C6", "Mobile & Accessories", "D3"), ("C7", "Computing", "D3"),
    ("C8", "Packaged Foods", "D4"), ("C9", "Beverages", "D4"),
]
REGIONS = [("R1", "Northeast"), ("R2", "Midwest"), ("R3", "South"), ("R4", "West")]
SUPPLIERS = [
    ("SUP1", "Northline Distribution", "USA"), ("SUP2", "Coastal Imports", "Vietnam"),
    ("SUP3", "Everline Manufacturing", "USA"), ("SUP4", "PrimeStock Wholesale", "Mexico"),
]
N_PRODUCTS = 30
N_STORES = 10
PAYMENT_TYPES = ["card", "cash", "mobile_wallet"]


def ensure_dir():
    os.makedirs(RAW_DIR, exist_ok=True)


def write_csv(filename, header, rows):
    path = os.path.join(RAW_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


def generate_departments():
    write_csv("departments.csv", ["department_id", "department_name"], DEPARTMENTS)


def generate_categories():
    write_csv("categories.csv", ["category_id", "category_name", "department_id"], CATEGORIES)
    return [c[0] for c in CATEGORIES]


def generate_regions():
    write_csv("regions.csv", ["region_id", "region_name"], REGIONS)
    return [r[0] for r in REGIONS]


def generate_suppliers():
    write_csv("suppliers.csv", ["supplier_id", "supplier_name", "country"], SUPPLIERS)
    return [s[0] for s in SUPPLIERS]


def generate_products(category_ids, supplier_ids):
    rows = []
    for pid in range(1, N_PRODUCTS + 1):
        rows.append([
            f"P{pid:04d}", f"Product {pid}", random.choice(category_ids),
            random.choice(supplier_ids), round(random.uniform(3.0, 120.0), 2),
        ])
    write_csv("products.csv", ["product_id", "product_name", "category_id", "supplier_id", "unit_cost"], rows)
    return [r[0] for r in rows]


def generate_stores(region_ids):
    rows = [[f"S{sid:03d}", f"Store {sid}", random.choice(region_ids)] for sid in range(1, N_STORES + 1)]
    write_csv("stores.csv", ["store_id", "store_name", "region_id"], rows)
    return [r[0] for r in rows]


def generate_transactions(product_ids, store_ids, start_date, n_days, n_per_day):
    rows = []
    txn_id = 1
    for d in range(n_days):
        day = start_date + timedelta(days=d)
        for _ in range(n_per_day):
            ts = day + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59))
            rows.append([
                f"TXN{txn_id:07d}", ts.isoformat(sep=" "), random.choice(store_ids),
                random.choice(product_ids), random.randint(1, 6),
                round(random.uniform(5.0, 180.0), 2), random.choice(PAYMENT_TYPES),
            ])
            txn_id += 1

    rows.append(["TXN0000001", "2026-06-01 10:15:00", store_ids[0], product_ids[0], 2, 19.99, "card"])
    rows.append(["TXN9999901", "2026-06-01 11:00:00", "", product_ids[0], 1, 9.99, "cash"])
    rows.append(["TXN9999902", "2026-06-01 11:05:00", store_ids[0], product_ids[0], "N/A", 9.99, "cash"])
    rows.append(["TXN9999903", "2026-06-01 11:10:00", store_ids[0], product_ids[0], 1, "", "cash"])

    write_csv(
        "transactions.csv",
        ["transaction_id", "transaction_ts", "store_id", "product_id", "quantity", "unit_price", "payment_type"],
        rows,
    )


def generate_supply_chain(product_ids, supplier_ids, start_date, n_shipments):
    rows = []
    for i in range(1, n_shipments + 1):
        ship_date = start_date + timedelta(days=random.randint(0, 20))
        rows.append([
            f"SHP{i:06d}", ship_date.date().isoformat(), random.choice(product_ids),
            random.choice(supplier_ids), random.randint(20, 500),
        ])
    write_csv("supply_chain.csv", ["shipment_id", "ship_date", "product_id", "supplier_id", "quantity_received"], rows)


if __name__ == "__main__":
    ensure_dir()
    generate_departments()
    category_ids = generate_categories()
    region_ids = generate_regions()
    supplier_ids = generate_suppliers()
    product_ids = generate_products(category_ids, supplier_ids)
    store_ids = generate_stores(region_ids)
    generate_transactions(product_ids, store_ids, datetime(2026, 6, 1), 14, 50)
    generate_supply_chain(product_ids, supplier_ids, datetime(2026, 6, 1), 200)
    print("\nSample data generated in data/raw/ (includes 4 planted DQ issues in transactions.csv)")
