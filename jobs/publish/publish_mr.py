"""Load MapReduce outputs (tab-separated text on HDFS) into MongoDB for serving."""

from datetime import datetime, timezone

from pyspark.sql import functions as F

from argus import config
from argus.spark import mongo_db, session


def read_tsv(spark, path, names):
    lines = spark.read.text(path).where(F.length("value") > 0)
    parts = F.split("value", "\t")
    return lines.select(*[parts.getItem(i).alias(n) for i, n in enumerate(names)])


def main():
    spark = session("publish-mr")
    db = mongo_db()
    now = datetime.now(timezone.utc)

    state_hour = read_tsv(spark, f"{config.MR_PATH}/state_hour", ["state", "hour", "accidents"]).collect()
    db.mr_state_hour.delete_many({})
    db.mr_state_hour.insert_many([
        {"state": r["state"], "hour": int(r["hour"]), "accidents": int(r["accidents"]), "loaded_at": now}
        for r in state_hour
    ])

    stats_docs = []
    for group in ("weather", "state", "hour"):
        path = f"{config.MR_PATH}/severity_by_{group}"
        for r in read_tsv(spark, path, ["key", "count", "avg", "severe"]).collect():
            stats_docs.append({"group_by": group, "key": r["key"], "accidents": int(r["count"]),
                               "avg_severity": float(r["avg"]), "severe_ratio": float(r["severe"]),
                               "loaded_at": now})
    db.mr_severity_stats.delete_many({})
    db.mr_severity_stats.insert_many(stats_docs)

    top = read_tsv(spark, f"{config.MR_PATH}/top_streets",
                   ["rank", "score", "count", "avg", "severe", "place"]).collect()
    db.mr_top_streets.delete_many({})
    db.mr_top_streets.insert_many([
        dict(zip(("street", "city", "state"), r["place"].rsplit("|", 2), strict=False),
             rank=int(r["rank"]), severity_score=int(r["score"]), accidents=int(r["count"]),
             avg_severity=float(r["avg"]), severe_ratio=float(r["severe"]), loaded_at=now)
        for r in top
    ])
    print(f"[argus] published {len(state_hour)} state-hour rows, {len(stats_docs)} severity stats, "
          f"{len(top)} top streets to MongoDB")
    spark.stop()


if __name__ == "__main__":
    main()
