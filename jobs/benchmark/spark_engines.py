"""Same query on four Spark APIs/formats; prints `BENCH,<engine>,<seconds>,<rows>` lines.

Query: accidents, average severity and severe ratio per state.
"""

import csv
import time

from pyspark.sql import functions as F

from argus import config
from argus.spark import session

SEVERITY, STATE = 2, 14


def timed(name, fn):
    t0 = time.perf_counter()
    rows = fn()
    print(f"BENCH,{name},{time.perf_counter() - t0:.3f},{rows}", flush=True)


def rdd_query(spark):
    def parse(line):
        if line.startswith("ID,"):
            return []
        f = next(csv.reader([line]))
        if len(f) != 46 or not f[SEVERITY].isdigit():
            return []
        s = int(f[SEVERITY])
        return [(f[STATE], (1, s, 1 if s >= 3 else 0))]

    return (spark.sparkContext.textFile(config.RAW_PATH)
            .flatMap(parse)
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
            .mapValues(lambda v: (v[0], v[1] / v[0], v[2] / v[0]))
            .count())


def raw_df(spark):
    return spark.read.option("header", True).option("escape", '"').csv(config.RAW_PATH)


def df_query(spark):
    sev = F.col("Severity").cast("int")
    return (raw_df(spark).groupBy("State")
            .agg(F.count("*"), F.avg(sev), F.avg((sev >= 3).cast("double"))).count())


def sql_query(spark):
    raw_df(spark).createOrReplaceTempView("accidents_raw")
    return spark.sql("""
        SELECT State, COUNT(*) AS n, AVG(CAST(Severity AS INT)) AS avg_sev,
               AVG(CASE WHEN CAST(Severity AS INT) >= 3 THEN 1.0 ELSE 0.0 END) AS severe
        FROM accidents_raw GROUP BY State""").count()


def parquet_query(spark):
    return (spark.read.parquet(config.CURATED_PATH).groupBy("state")
            .agg(F.count("*"), F.avg("severity"), F.avg((F.col("severity") >= 3).cast("double"))).count())


def main():
    spark = session("benchmark")
    timed("spark_rdd_csv", lambda: rdd_query(spark))
    timed("spark_dataframe_csv", lambda: df_query(spark))
    timed("spark_sql_csv", lambda: sql_query(spark))
    timed("spark_dataframe_parquet", lambda: parquet_query(spark))
    spark.stop()


if __name__ == "__main__":
    main()
