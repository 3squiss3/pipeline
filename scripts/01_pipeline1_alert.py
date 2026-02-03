import os, time, argparse
from datetime import datetime
from scripts.db import get_db

DROP = float(os.getenv("ALERT_DROP_PCT","0.10"))

def get_last_ts(db):
    st = db.pipeline_state.find_one({"_id":"p1"})
    return st.get("last_ts") if st else None

def set_last_ts(db, ts):
    db.pipeline_state.update_one({"_id":"p1"}, {"$set":{"last_ts": ts}}, upsert=True)

def run_once():
    db = get_db()
    last_ts = get_last_ts(db)

    q = {} if not last_ts else {"ts":{"$gt": last_ts}}
    new_events = list(db.price_events.find(q, {"_id":0}).sort("ts", 1))
    if not new_events:
        return

    for e in new_events:
        pid = e["product_id"]
        snap = db.metrics_snapshot.find_one({"product_id":pid}, {"_id":0})
        if not snap or not snap.get("median_4w"):
            set_last_ts(db, e["ts"])
            continue

        med = snap["median_4w"]
        min12 = snap.get("min_12w")
        price = e["price"]

        is_drop = price <= (1-DROP)*med
        is_near_min = (min12 is not None) and (price <= 1.02*min12)

        if is_drop or is_near_min:
            alert = {
                "product_id": pid,
                "merchant": e["merchant"],
                "price": price,
                "ts": e["ts"],
                "rule": "drop_vs_median" if is_drop else "near_12w_min",
                "median_4w": med,
                "min_12w": min12
            }
            # unique index => pas d'alerte fantôme si re-run
            try:
                db.alerts.insert_one(alert)
            except Exception:
                pass

            db.products_curated.update_one(
                {"product_id":pid},
                {"$set":{"banner":"Acheter maintenant","last_alert_ts": e["ts"],"updated_at": datetime.utcnow()}}
            )
            print(f"[ALERT] {pid} {price}€ ({alert['rule']})")

        set_last_ts(db, e["ts"])

def main(loop):
    if loop:
        while True:
            run_once()
            time.sleep(loop)
    else:
        run_once()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0)
    args = ap.parse_args()
    main(args.loop if args.loop>0 else None)
