"""FP-Growth association rules: which conditions co-occur with severe accidents (Unit IV).

Each accident becomes a basket of condition items, e.g.
  {weather=Rain, light=Night, road=Junction, vis=Low, sev=High}
and we keep rules whose consequent is sev=High, ranked by lift.
"""

import argparse
from datetime import datetime, timezone

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from argus import config
from argus.spark import mlflow_run, mongo_db, session


def baskets(df):
    item = lambda cond, name: F.when(cond, F.lit(name))  # noqa: E731
    items = F.array(
        F.concat(F.lit("weather="), F.col("weather_bucket")),
        item(F.col("is_night") == 1, "light=Night"),
        item(F.col("is_night") == 0, "light=Day"),
        item(F.col("junction") == 1, "road=Junction"),
        item(F.col("crossing") == 1, "road=Crossing"),
        item(F.col("traffic_signal") == 1, "road=Signal"),
        item(F.col("stop") == 1, "road=Stop"),
        item(F.col("railway") == 1, "road=Railway"),
        item(F.col("visibility_mi") < 2, "vis=Low"),
        item(F.col("wind_speed_mph") > 20, "wind=High"),
        item(F.col("temperature_f") <= 32, "temp=Freezing"),
        item(F.col("hour").between(7, 9) | F.col("hour").between(16, 19), "time=RushHour"),
        item(F.col("day_of_week").isin(1, 7), "day=Weekend"),
        F.when(F.col("severity") >= 3, F.lit("sev=High")).otherwise(F.lit("sev=Low")),
    )
    return df.select(F.array_distinct(F.filter(items, lambda x: x.isNotNull())).alias("items"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-support", type=float, default=0.005)
    p.add_argument("--min-confidence", type=float, default=0.2)
    p.add_argument("--fraction", type=float, default=1.0)
    a = p.parse_args()

    spark = session("rules-fpgrowth")
    df = spark.read.parquet(config.CURATED_PATH)
    if a.fraction < 1.0:
        df = df.sample(fraction=a.fraction, seed=5)
    data = baskets(df).cache()
    n = data.count()

    with mlflow_run("rules-fpgrowth") as log:
        model = FPGrowth(itemsCol="items", minSupport=a.min_support, minConfidence=a.min_confidence).fit(data)
        rules = (
            model.associationRules
            .where(F.array_contains("consequent", "sev=High"))
            .where(~F.exists("antecedent", lambda x: x.startswith("sev=")))
            .orderBy(F.desc("lift"))
        )
        rules.write.mode("overwrite").parquet(f"{config.RESULTS_PATH}/rules_severe")
        top = rules.limit(50).collect()

        docs = [{
            "antecedent": sorted(r["antecedent"]),
            "consequent": list(r["consequent"]),
            "confidence": round(r["confidence"], 4),
            "lift": round(r["lift"], 4),
            "support": round(r["support"], 5),
            "rule": " AND ".join(sorted(r["antecedent"])) + " => severe",
        } for r in top]
        db = mongo_db()
        db.rules.delete_many({})
        if docs:
            now = datetime.now(timezone.utc)
            db.rules.insert_many([{**d, "mlflow_run_id": log.run_id, "created_at": now} for d in docs])

        log.params(min_support=a.min_support, min_confidence=a.min_confidence, baskets=n)
        log.metrics(frequent_itemsets=model.freqItemsets.count(), severe_rules=rules.count(),
                    best_lift=docs[0]["lift"] if docs else 0)
        log.json("top_rules.json", [{k: v for k, v in d.items() if k != "_id"} for d in docs[:20]])

    for d in docs[:10]:
        print(f"[argus] lift={d['lift']:.2f} conf={d['confidence']:.2f}  {d['rule']}")
    spark.stop()


if __name__ == "__main__":
    main()
