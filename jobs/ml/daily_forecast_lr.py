"""Linear Regression: forecast tomorrow's accident count per city (Unit IV: regression).

Daily series per top-N city with missing days filled as 0, then lag features
built with window functions: yesterday, same weekday last week, 7-day mean.
Time-based split (last --test-days held out) - never a random split for time series.
"""

import argparse
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Window
from pyspark.sql import functions as F

from argus import config
from argus.spark import mlflow_run, mongo_db, session


def daily_series(df, top_n):
    top = (df.groupBy("city", "state").count().orderBy(F.desc("count")).limit(top_n)
           .select(F.concat_ws(", ", "city", "state").alias("place")))
    daily = (df.withColumn("place", F.concat_ws(", ", "city", "state"))
             .join(top, "place").groupBy("place", "date").agg(F.count("*").alias("cnt")))
    bounds = daily.groupBy("place").agg(F.min("date").alias("d0"), F.max("date").alias("d1"))
    last = daily.agg(F.max("date")).first()[0]
    # One extra (unknown) day per city: the day we forecast.
    grid = bounds.select(
        "place",
        F.explode(F.sequence("d0", F.date_add(F.lit(last), 1))).alias("date"),
    )
    return grid.join(daily, ["place", "date"], "left").na.fill(0, ["cnt"]).withColumn(
        "cnt", F.when(F.col("date") > F.lit(last), F.lit(None)).otherwise(F.col("cnt"))
    ), last


def add_lags(series):
    w = Window.partitionBy("place").orderBy("date")
    return (
        series.withColumn("lag1", F.lag("cnt", 1).over(w))
        .withColumn("lag7", F.lag("cnt", 7).over(w))
        .withColumn("mean7", F.avg("cnt").over(w.rowsBetween(-7, -1)))
        .withColumn("mean28", F.avg("cnt").over(w.rowsBetween(-28, -1)))
        .withColumn("dow", F.dayofweek("date").cast("string"))
        .withColumn("month", F.month("date").cast("double"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7).cast("double"))
        .where(F.col("lag7").isNotNull())
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top-cities", type=int, default=25)
    p.add_argument("--test-days", type=int, default=90)
    a = p.parse_args()

    spark = session("daily-forecast-lr")
    df = spark.read.parquet(config.CURATED_PATH).select("city", "state", "date")
    series, last_day = daily_series(df, a.top_cities)
    feats = add_lags(series).cache()

    known = feats.where(F.col("cnt").isNotNull()).withColumn("label", F.col("cnt").cast("double"))
    cutoff = F.date_sub(F.lit(last_day), a.test_days)
    train = known.where(F.col("date") <= cutoff)
    test = known.where(F.col("date") > cutoff)

    pipeline = Pipeline(stages=[
        StringIndexer(inputCols=["place", "dow"], outputCols=["place_idx", "dow_idx"], handleInvalid="keep"),
        OneHotEncoder(inputCols=["place_idx", "dow_idx"], outputCols=["place_vec", "dow_vec"]),
        VectorAssembler(inputCols=["lag1", "lag7", "mean7", "mean28", "month", "is_weekend",
                                   "place_vec", "dow_vec"], outputCol="features"),
        LinearRegression(featuresCol="features", labelCol="label", regParam=0.1, elasticNetParam=0.2),
    ])

    with mlflow_run("daily-forecast-lr") as log:
        model = pipeline.fit(train)
        pred = model.transform(test)
        metrics = {m: RegressionEvaluator(labelCol="label", metricName=m).evaluate(pred)
                   for m in ("rmse", "mae", "r2")}
        naive = RegressionEvaluator(labelCol="label", predictionCol="lag7", metricName="mae").evaluate(test)
        metrics["mae_naive_lastweek"] = naive
        lr = model.stages[-1]
        log.params(top_cities=a.top_cities, test_days=a.test_days, regParam=0.1, elasticNet=0.2)
        log.metrics(**metrics, train_r2=lr.summary.r2)
        model.write().overwrite().save(config.FORECAST_MODEL_PATH)

        tomorrow = model.transform(feats.where(F.col("cnt").isNull())).select(
            "place", "date", F.greatest(F.lit(0.0), F.col("prediction")).alias("predicted"), "mean7"
        ).collect()
        db = mongo_db()
        now = datetime.now(timezone.utc)
        for r in tomorrow:
            db.forecasts.update_one(
                {"place": r["place"], "date": str(r["date"])},
                {"$set": {"predicted_accidents": round(r["predicted"], 1),
                          "trailing_7d_mean": round(r["mean7"] or 0, 1),
                          "model": "linear_regression", "mlflow_run_id": log.run_id, "created_at": now}},
                upsert=True,
            )
        db.models.update_one({"name": "daily_forecast_lr"}, {"$set": {
            "name": "daily_forecast_lr", "metrics": {k: round(v, 4) for k, v in metrics.items()},
            "mlflow_run_id": log.run_id, "trained_at": now}}, upsert=True)

    print(f"[argus] test metrics {metrics} (naive last-week MAE {naive:.2f}); "
          f"{len(tomorrow)} forecasts for {last_day} + 1 day")
    spark.stop()


if __name__ == "__main__":
    main()
