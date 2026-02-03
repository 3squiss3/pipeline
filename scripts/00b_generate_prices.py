import random
from datetime import datetime, timedelta
from scripts.db import get_db

MERCHANTS = ["Amazon","Fnac","Cdiscount"]
BASE = {"P001":899.0,"P002":499.0,"P003":349.0}

def main(n=20, days_back=28):
    db = get_db()
    products = list(db.products.find({}, {"_id":0}))
    now = datetime.utcnow()

    for _ in range(n):
        p = random.choice(products)
        pid = p["product_id"]
        ts = now - timedelta(days=random.random()*days_back, hours=random.random()*24)

        noise = random.uniform(-0.06, 0.06)
        promo = 0.88 if random.random() < 0.10 else 1.0
        price = round(BASE[pid] * (1+noise) * promo, 2)

        db.price_events.insert_one({
            "product_id": pid,
            "merchant": random.choice(MERCHANTS),
            "price": price,
            "currency": "EUR",
            "ts": ts
        })

    print(f"Generated {n} events.")

if __name__ == "__main__":
    import sys
    main(n=int(sys.argv[1]) if len(sys.argv)>1 else 20)
