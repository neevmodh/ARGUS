"""Decision Tree: predict accident severity (1-4) from conditions (Unit IV: classification).

Severity 2 is ~80% of the data, so classes are re-weighted (inverse frequency)
or the tree would just predict "2" and look 80% accurate. The fitted pipeline
(feature stages + tree) is saved to HDFS and reused by the streaming scorer.
"""

import argparse
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F

from argus import config
from argus.features import cast_numeric, feature_stages
from argus.schema import CATEGORICAL_FEATURES, NUMERIC_FEATURES
from argus.spark import mlflow_run, mongo_db, session


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--max-depth", type=int, default=10)
    p.add_argument("--tune", action="store_true", help="grid-search maxDepth/minInstancesPerNode")
    p.add_argument("--fraction", type=float, default=1.0, help="train on a sample for speed")
    a = p.parse_args()

    spark = session("severity-tree")
    df = spark.read.parquet(config.CURATED_PATH)
    if a.fraction < 1.0:
        df = df.sample(fraction=a.fraction, seed=11)
    df = cast_numeric(df.select("severity", *NUMERIC_FEATURES, *CATEGORICAL_FEATURES))
    df = df.withColumn("label", (F.col("severity") - 1).cast("double"))

    counts = {r["label"]: r["count"] for r in df.groupBy("label").count().collect()}
    total = sum(counts.values())
    weight_expr = F.create_map(*[x for lbl, c in counts.items() for x in (F.lit(lbl), F.lit(total / (len(counts) * c)))])
    df = df.withColumn("weight", weight_expr[F.col("label")])

    train, test = df.randomSplit([0.8, 0.2], seed=11)
    train.cache()

    tree = DecisionTreeClassifier(labelCol="label", featuresCol="features", weightCol="weight",
                                  maxDepth=a.max_depth, maxBins=64, minInstancesPerNode=20, seed=11)
    pipeline = Pipeline(stages=feature_stages() + [tree])

    f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    with mlflow_run("severity-tree") as log:
        if a.tune:
            grid = (ParamGridBuilder()
                    .addGrid(tree.maxDepth, [6, 10, 14])
                    .addGrid(tree.minInstancesPerNode, [5, 20, 100])
                    .build())
            tvs = TrainValidationSplit(estimator=pipeline, estimatorParamMaps=grid, evaluator=f1,
                                       trainRatio=0.8, parallelism=2, seed=11)
            tuned = tvs.fit(train)
            for params, score in zip(grid, tuned.validationMetrics, strict=True):
                print(f"[argus] {({p.name: v for p, v in params.items()})} f1={score:.4f}")
            model = tuned.bestModel
        else:
            model = pipeline.fit(train)

        pred = model.transform(test).cache()
        metrics = {
            name: MulticlassClassificationEvaluator(labelCol="label", metricName=name).evaluate(pred)
            for name in ("accuracy", "f1", "weightedPrecision", "weightedRecall")
        }
        for lbl in sorted(counts):
            metrics[f"recall_sev{int(lbl) + 1}"] = MulticlassClassificationEvaluator(
                labelCol="label", metricName="recallByLabel", metricLabel=lbl).evaluate(pred)

        confusion = (
            pred.groupBy("label").pivot("prediction", [0.0, 1.0, 2.0, 3.0]).count().na.fill(0)
            .orderBy("label").collect()
        )
        confusion = {f"actual_{int(r['label']) + 1}": [int(r[str(c)]) for c in (0.0, 1.0, 2.0, 3.0)] for r in confusion}

        dt_model = model.stages[-1]
        names = [f"{c}" for c in NUMERIC_FEATURES] + CATEGORICAL_FEATURES
        importances = sorted(
            ((names[i], float(v)) for i, v in enumerate(dt_model.featureImportances.toArray())),
            key=lambda kv: -kv[1],
        )

        model.write().overwrite().save(config.SEVERITY_MODEL_PATH)

        chosen = {"maxDepth": dt_model.getMaxDepth(), "minInstancesPerNode": dt_model.getMinInstancesPerNode()}
        log.params(tuned=a.tune, fraction=a.fraction, train_rows=train.count(), **chosen)
        log.metrics(**metrics, tree_nodes=dt_model.numNodes, tree_depth=dt_model.depth)
        log.json("confusion_matrix.json", confusion)
        log.json("feature_importance.json", importances)
        log.tag("model_path", config.SEVERITY_MODEL_PATH)

        mongo_db().models.update_one(
            {"name": "severity_tree"},
            {"$set": {
                "name": "severity_tree",
                "path": config.SEVERITY_MODEL_PATH,
                "metrics": {k: round(v, 4) for k, v in metrics.items()},
                "params": chosen,
                "confusion_matrix": confusion,
                "feature_importance": importances[:15],
                "mlflow_run_id": log.run_id,
                "trained_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )

    print("[argus] metrics:", {k: round(v, 4) for k, v in metrics.items()})
    print("[argus] top features:", importances[:8])
    spark.stop()


if __name__ == "__main__":
    main()
