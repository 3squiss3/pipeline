import os
from datetime import datetime, timedelta
from scripts.db import get_db

DROP = float(os.getenv("ALERT_DROP_PCT","0.10"))

def main():
    db = get_db()
    now = datetime.utcnow()
    since_7d = now - timedelta(days=7)

    db.product_views.delete_many({})

    for p in db.products.find({}, {"_id":0}):
        pid = p["product_id"]
        snap = db.metrics_snapshot.find_one({"product_id":pid}, {"_id":0}) or {}
        med_4w = snap.get("median_4w")

        ev = list(db.price_events.find({"product_id":pid, "ts":{"$gte": since_7d}}, {"_id":0}).sort("ts",-1))
        if not ev:
            continue

        cleaned = []
        for e in ev:
            if e.get("currency") != "EUR":
                continue
            if med_4w and (e["price"] < 0.5*med_4w or e["price"] > 1.5*med_4w):
                continue
            cleaned.append(e)
        if not cleaned:
            continue

        latest = {}
        for e in cleaned:
            latest.setdefault(e["merchant"], e)

        best = min(latest.values(), key=lambda x: x["price"])
        verdict = "Surveiller"
        if med_4w and best["price"] <= (1-DROP)*med_4w:
            verdict = "Acheter maintenant"

        view = {
            "product_id": pid,
            "name": p["name"],
            "best_offer": {"merchant": best["merchant"], "price": best["price"], "ts": best["ts"]},
            "median_4w": med_4w,
            "verdict": verdict,
            "published_at": now
        }
        db.product_views.insert_one(view)

        db.products_curated.update_one(
            {"product_id":pid},
            {"$set": {"last_price": best["price"], "last_seen": best["ts"], "banner": verdict, "updated_at": now}}
        )

    print("Pipeline 2 OK -> product_views + products_curated.")

if __name__ == "__main__":
    main()
