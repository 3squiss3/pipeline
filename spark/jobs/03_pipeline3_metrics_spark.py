import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as smin, expr, stddev_pop, percentile_approx, max as smax

MONGO_URI = os.environ["MONGO_URI"]
DB_NAME = os.getenv("DB_NAME","price_demo")

def main():
    from pymongo import MongoClient
    db = MongoClient(MONGO_URI)[DB_NAME]

    events = list(db.price_events.find({}, {"_id":0}))
    if not events:
        print("No events.")
        return

    spark = SparkSession.builder.appName("metrics").getOrCreate()
    df = spark.createDataFrame(events)

    now = datetime.utcnow()
    ts_12w = now - timedelta(days=84)
    ts_4w  = now - timedelta(days=28)
    ts_2w  = now - timedelta(days=14)

    # On filtre par fenêtres temporelles (en gardant le code simple)
    df12 = df.where(col("ts") >= ts_12w)
    df4  = df.where(col("ts") >= ts_4w)
    df2  = df.where(col("ts") >= ts_2w)

    # min_12w, median_4w, volatility_4w
    min12 = df12.groupBy("product_id").agg(smin("price").alias("min_12w"))
    med4  = df4.groupBy("product_id").agg(percentile_approx("price", 0.5).alias("median_4w"),
                                         stddev_pop("price").alias("volatility_4w"))
    # trend_2w simple: (last - first) / first basé sur ts
    first_last = df2.groupBy("product_id").agg(
        expr("min_by(price, ts)").alias("first_price"),
        expr("max_by(price, ts)").alias("last_price")
    ).withColumn("trend_2w", (col("last_price")-col("first_price"))/col("first_price"))

    out = min12.join(med4, "product_id", "outer").join(first_last.select("product_id","trend_2w"), "product_id", "outer")
    rows = [r.asDict() for r in out.collect()]
    for r in rows:
        r["computed_at"] = now
        db.metrics_snapshot.update_one({"product_id": r["product_id"]}, {"$set": r}, upsert=True)

    print("Pipeline 3 OK (Spark) -> metrics_snapshot.")

if __name__ == "__main__":
    main()
