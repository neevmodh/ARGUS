"""Raw CSV (HDFS) -> typed, cleaned Parquet partitioned by state/year (HDFS).

Everything is read as string and cast explicitly: schema inference over 3 GB
would cost a full extra pass over the data.
"""

import argparse

from pyspark.sql import functions as F

from argus import config
from argus.schema import RAW_TO_ROAD_FLAG
from argus.spark import mlflow_run, session
from argus.weather import bucket_col


def clean(raw):
    ts = lambda c: F.to_timestamp(F.substring(F.col(c), 1, 19), "yyyy-MM-dd HH:mm:ss")  # noqa: E731
    num = lambda c: F.col(c).cast("double")  # noqa: E731

    df = raw.select(
        F.col("ID").alias("id"),
        F.col("Severity").cast("int").alias("severity"),
        ts("Start_Time").alias("start_time"),
        ts("End_Time").alias("end_time"),
        num("Start_Lat").alias("lat"),
        num("Start_Lng").alias("lng"),
        num("Distance(mi)").alias("distance_mi"),
        F.trim("Street").alias("street"),
        F.trim("City").alias("city"),
        F.trim("County").alias("county"),
        F.trim("State").alias("state"),
        F.col("Timezone").alias("timezone"),
        num("Temperature(F)").alias("temperature_f"),
        num("Humidity(%)").alias("humidity"),
        num("Pressure(in)").alias("pressure_in"),
        num("Visibility(mi)").alias("visibility_mi"),
        num("Wind_Speed(mph)").alias("wind_speed_mph"),
        F.coalesce(num("Precipitation(in)"), F.lit(0.0)).alias("precipitation_in"),
        F.col("Weather_Condition").alias("weather_condition"),
        bucket_col(F.col("Weather_Condition")).alias("weather_bucket"),
        (F.col("Sunrise_Sunset") == "Night").cast("int").alias("is_night"),
        *[(F.col(raw_name) == "True").cast("int").alias(flag) for raw_name, flag in RAW_TO_ROAD_FLAG.items()],
    )

    df = (
        df.where(
            F.col("severity").between(1, 4)
            & F.col("start_time").isNotNull()
            & F.col("lat").between(18, 72)
            & F.col("lng").between(-180, -60)
            & (F.length("state") == 2)
        )
        .withColumn("year", F.year("start_time"))
        .withColumn("month", F.month("start_time"))
        .withColumn("hour", F.hour("start_time"))
        .withColumn("day_of_week", F.dayofweek("start_time"))
        .withColumn("date", F.to_date("start_time"))
        .withColumn(
            "duration_min",
            (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60.0,
        )
        .dropDuplicates(["id"])
    )
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=config.RAW_PATH)
    p.add_argument("--output", default=config.CURATED_PATH)
    a = p.parse_args()

    spark = session("ingest-clean")
    raw = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "DROPMALFORMED")
        .csv(a.input)
    )
    raw_count = raw.count()
    curated = clean(raw).cache()
    kept = curated.count()

    curated.repartition("state").write.mode("overwrite").partitionBy("state", "year").parquet(a.output)

    by_severity = {r["severity"]: r["count"] for r in curated.groupBy("severity").count().collect()}
    with mlflow_run("ingest-clean") as log:
        log.params(input=a.input, output=a.output)
        log.metrics(raw_rows=raw_count, curated_rows=kept, dropped_rows=raw_count - kept)
        log.json("severity_distribution.json", by_severity)
    print(f"[argus] raw={raw_count:,} curated={kept:,} dropped={raw_count - kept:,} -> {a.output}")
    spark.stop()


if __name__ == "__main__":
    main()
