"""Speed layer: Kafka -> Spark Structured Streaming -> MongoDB (+ Kafka alerts).

Three concurrent streaming queries over the same scored stream:
  1. every event with its risk score        -> MongoDB  argus.live_events   (foreachBatch)
  2. 1-minute tumbling windows per state    -> MongoDB  argus.live_windows  (watermark + update mode)
  3. high-risk events (risk >= threshold)   -> Kafka    accidents.alerts
The severity model is the Decision Tree pipeline trained by jobs/ml/severity_tree.py.
"""

import argparse
from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql import types as T

from argus import config
from argus.features import add_time_features, cast_numeric
from argus.schema import ROAD_FLAGS
from argus.spark import mongo_db, session

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("local_time", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lng", T.DoubleType()),
        T.StructField("city", T.StringType()),
        T.StructField("state", T.StringType()),
        T.StructField("street", T.StringType()),
        T.StructField("severity_actual", T.IntegerType()),
        T.StructField("temperature_f", T.DoubleType()),
        T.StructField("humidity", T.DoubleType()),
        T.StructField("visibility_mi", T.DoubleType()),
        T.StructField("wind_speed_mph", T.DoubleType()),
        T.StructField("precipitation_in", T.DoubleType()),
        T.StructField("weather_condition", T.StringType()),
        T.StructField("weather_bucket", T.StringType()),
        T.StructField("weather_source", T.StringType()),
        T.StructField("is_night", T.IntegerType()),
    ]
    + [T.StructField(f, T.IntegerType()) for f in ROAD_FLAGS]
)

_db = None


def db():
    global _db
    if _db is None:
        _db = mongo_db()
    return _db


def write_events(batch, batch_id):
    rows = batch.collect()
    if not rows:
        return
    now = datetime.now(timezone.utc)
    docs = [
        {
            "event_id": r["id"],
            "event_time": r["event_time"],
            "ingested_at": now,
            "location": {"type": "Point", "coordinates": [r["lng"], r["lat"]]},
            "city": r["city"],
            "state": r["state"],
            "street": r["street"],
            "weather": r["weather_bucket"],
            "weather_source": r["weather_source"],
            "is_night": bool(r["is_night"]),
            "predicted_severity": int(r["predicted_severity"]),
            "severity_actual": r["severity_actual"],
            "p_severe": round(float(r["p_severe"]), 4),
            "risk_score": float(r["risk_score"]),
            "batch_id": batch_id,
        }
        for r in rows
    ]
    db().live_events.insert_many(docs, ordered=False)
    print(f"[argus] batch {batch_id}: {len(docs)} events scored")


def write_windows(batch, batch_id):
    for r in batch.collect():
        db().live_windows.update_one(
            {"state": r["state"], "window_start": r["window"]["start"]},
            {"$set": {
                "window_end": r["window"]["end"],
                "events": int(r["events"]),
                "avg_risk": round(float(r["avg_risk"]), 2),
                "max_risk": float(r["max_risk"]),
                "high_risk_events": int(r["high_risk_events"]),
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--alert-threshold", type=float, default=70.0)
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--starting-offsets", default="latest")
    a = p.parse_args()

    spark = session("live-risk")
    model = PipelineModel.load(config.SEVERITY_MODEL_PATH)

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", a.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("local_ts", F.to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss"))
        .where(F.col("event_time").isNotNull() & F.col("lat").isNotNull())
    )
    events = cast_numeric(add_time_features(events, "local_ts"))

    scored = (
        model.transform(events)
        .withColumn("proba", vector_to_array("probability"))
        # labels are severity-1, so indices 2.. are severity 3 and 4
        .withColumn("p_severe", F.expr("aggregate(slice(proba, 3, size(proba)), 0D, (acc, x) -> acc + x)"))
        .withColumn("predicted_severity", (F.col("prediction") + 1).cast("int"))
        .withColumn("risk_score", F.round(100 * F.col("p_severe"), 1))
    )

    ckpt = config.CHECKPOINT_PATH
    q_events = (
        scored.select("id", F.col("event_time").cast("string").alias("event_time"), "lat", "lng", "city",
                      "state", "street", "weather_bucket", "weather_source", "is_night",
                      "predicted_severity", "severity_actual", "p_severe", "risk_score")
        .writeStream.queryName("live_events").foreachBatch(write_events)
        .option("checkpointLocation", f"{ckpt}/live_events").trigger(processingTime=a.trigger).start()
    )

    q_windows = (
        scored.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "state")
        .agg(
            F.count("*").alias("events"),
            F.avg("risk_score").alias("avg_risk"),
            F.max("risk_score").alias("max_risk"),
            F.sum((F.col("risk_score") >= a.alert_threshold).cast("int")).alias("high_risk_events"),
        )
        .writeStream.queryName("live_windows").outputMode("update").foreachBatch(write_windows)
        .option("checkpointLocation", f"{ckpt}/live_windows").trigger(processingTime=a.trigger).start()
    )

    q_alerts = (
        scored.where(F.col("risk_score") >= a.alert_threshold)
        .select(
            F.col("state").alias("key"),
            F.to_json(F.struct("id", "event_time", "city", "state", "street", "lat", "lng",
                               "weather_bucket", "predicted_severity", "risk_score")).alias("value"),
        )
        .writeStream.queryName("alerts").format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("topic", f"{config.KAFKA_TOPIC.rsplit('.', 1)[0]}.alerts")
        .option("checkpointLocation", f"{ckpt}/alerts").trigger(processingTime=a.trigger).start()
    )

    print(f"[argus] streaming: {[q.name for q in (q_events, q_windows, q_alerts)]} "
          f"(alerts at risk >= {a.alert_threshold})")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
