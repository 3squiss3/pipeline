from datetime import datetime
from scripts.db import get_db


def main():
    db = get_db()
    for c in [
        "products",
        "products_curated",
        "price_events",
        "metrics_snapshot",
        "product_views",
        "alerts",
        "pipeline_state",
    ]:
        db[c].delete_many({})

    products = [
        {"product_id": "P001", "name": "iPhone 15", "category": "smartphone"},
        {"product_id": "P002", "name": "Dyson V11", "category": "aspirateur"},
        {"product_id": "P003", "name": "Switch OLED", "category": "console"},
    ]
    db.products.insert_many(products)
    now = datetime.utcnow()
    db.products_curated.insert_many(
        [{**p, "banner": "Surveiller", "updated_at": now} for p in products]
    )

    db.products.create_index("product_id", unique=True)
    db.products_curated.create_index("product_id", unique=True)
    db.price_events.create_index([("product_id", 1), ("ts", -1)])
    db.metrics_snapshot.create_index("product_id", unique=True)
    db.alerts.create_index([("product_id", 1), ("merchant", 1), ("ts", 1)], unique=True)

    print("Seed OK (3 produits).")


if __name__ == "__main__":
    main()
