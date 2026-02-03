from scripts.db import get_db

db = get_db()
print("products:", db.products.count_documents({}))
print("price_events:", db.price_events.count_documents({}))
print("metrics_snapshot:", db.metrics_snapshot.count_documents({}))
print("product_views:", db.product_views.count_documents({}))
print("alerts:", db.alerts.count_documents({}))
