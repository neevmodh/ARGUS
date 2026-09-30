"""K-Means on accident GPS points -> accident hotspots (Unit IV: clustering).

1. Elbow + silhouette sweep over candidate k on a sample (logged to MLflow).
2. Final model on the full data, clusters summarised into hotspots.
3. Hotspots written to HDFS (Parquet) and MongoDB (GeoJSON, 2dsphere-indexed).
"""

import argparse

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F

from argus import config
from argus.spark import mlflow_run, mongo_db, session


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--k", type=int, default=0, help="fixed k (0 = pick from --candidates)")
    p.add_argument("--candidates", default="50,100,150,200")
    p.add_argument("--sweep-fraction", type=float, default=0.1)
    p.add_argument("--min-accidents", type=int, default=30)
    a = p.parse_args()

    spark = session("hotspots-kmeans")
    df = spark.read.parquet(config.CURATED_PATH).select(
        "lat", "lng", "severity", "city", "state", "street"
    )
    points = VectorAssembler(inputCols=["lat", "lng"], outputCol="features").transform(df).cache()

    with mlflow_run("hotspots-kmeans") as log:
        k = a.k
        if not k:
            sample = points.sample(fraction=a.sweep_fraction, seed=7).cache()
            evaluator = ClusteringEvaluator()
            best = (-1.0, None)
            for cand in [int(x) for x in a.candidates.split(",")]:
                m = KMeans(k=cand, seed=7, maxIter=30).fit(sample)
                sil = evaluator.evaluate(m.transform(sample))
                cost = m.summary.trainingCost
                log.metrics(step=cand, silhouette=sil, wssse=cost)
                print(f"[argus] k={cand:<4} silhouette={sil:.4f} wssse={cost:,.1f}")
                if sil > best[0]:
                    best = (sil, cand)
            k = best[1]
            sample.unpersist()

        model = KMeans(k=k, seed=7, maxIter=40).fit(points)
        model.write().overwrite().save(config.KMEANS_MODEL_PATH)
        assigned = model.transform(points)

        top_city = (
            assigned.groupBy("prediction", "city", "state").count()
            .withColumn("r", F.row_number().over(Window.partitionBy("prediction").orderBy(F.desc("count"))))
            .where("r = 1").select("prediction", "city", "state")
        )
        top_street = (
            assigned.groupBy("prediction", "street").count()
            .withColumn("r", F.row_number().over(Window.partitionBy("prediction").orderBy(F.desc("count"))))
            .where("r = 1").select("prediction", F.col("street").alias("top_street"))
        )

        hotspots = (
            assigned.groupBy("prediction")
            .agg(
                F.count("*").alias("accidents"),
                F.avg("severity").alias("avg_severity"),
                F.avg((F.col("severity") >= 3).cast("double")).alias("severe_ratio"),
                F.avg("lat").alias("lat"),
                F.avg("lng").alias("lng"),
                F.stddev("lat").alias("lat_sd"),
                F.stddev("lng").alias("lng_sd"),
            )
            .where(F.col("accidents") >= a.min_accidents)
            .join(top_city, "prediction")
            .join(top_street, "prediction")
            .withColumnRenamed("prediction", "cluster_id")
        )
        max_acc = hotspots.agg(F.max("accidents")).first()[0] or 1
        hotspots = hotspots.withColumn(
            "risk_index",
            F.round(100 * (F.col("accidents") / F.lit(max_acc)) * (F.col("avg_severity") / 4), 2),
        ).orderBy(F.desc("risk_index"))
        hotspots.write.mode("overwrite").parquet(f"{config.RESULTS_PATH}/hotspots")

        rows = hotspots.collect()
        docs = [
            {
                "cluster_id": int(r["cluster_id"]),
                "location": {"type": "Point", "coordinates": [float(r["lng"]), float(r["lat"])]},
                "accidents": int(r["accidents"]),
                "avg_severity": round(float(r["avg_severity"]), 3),
                "severe_ratio": round(float(r["severe_ratio"]), 4),
                "radius_km": round(111 * max(r["lat_sd"] or 0, r["lng_sd"] or 0), 2),
                "risk_index": float(r["risk_index"]),
                "city": r["city"],
                "state": r["state"],
                "top_street": r["top_street"],
                "model_run_id": log.run_id,
            }
            for r in rows
        ]
        db = mongo_db()
        db.hotspots.delete_many({})
        if docs:
            db.hotspots.insert_many(docs)

        log.params(k=k, min_accidents=a.min_accidents)
        log.metrics(final_wssse=model.summary.trainingCost, hotspots=len(docs))
        log.json("top_hotspots.json", [{k2: v for k2, v in d.items() if k2 != "_id"} for d in docs[:20]])
    print(f"[argus] k={k} -> {len(docs)} hotspots written to MongoDB argus.hotspots")
    spark.stop()


if __name__ == "__main__":
    main()
