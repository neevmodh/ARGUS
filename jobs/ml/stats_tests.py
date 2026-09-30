"""NumPy/SciPy statistics on Spark aggregates (Unit IV: NumPy, SciPy).

Spark does the heavy aggregation; only small contingency tables / samples are
pulled to the driver for SciPy. Also produces `weather_risk` (relative risk of
a severe accident per weather bucket) used by the dashboard's risk checker.
"""

import argparse
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import functions as F
from scipy import stats

from argus import config
from argus.spark import mlflow_run, mongo_db, session


def contingency(df, row_col, col_col):
    pivot = df.groupBy(row_col).pivot(col_col).count().na.fill(0).orderBy(row_col).collect()
    cols = [c for c in pivot[0].asDict() if c != row_col]
    return [r[row_col] for r in pivot], cols, np.array([[r[c] for c in cols] for r in pivot], dtype=float)


def cramers_v(chi2, table):
    n = table.sum()
    r, k = table.shape
    return float(np.sqrt(chi2 / (n * (min(r, k) - 1))))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sample-size", type=int, default=200_000)
    a = p.parse_args()

    spark = session("stats-tests")
    df = spark.read.parquet(config.CURATED_PATH).withColumn(
        "severe", (F.col("severity") >= 3).cast("int")
    ).cache()
    total = df.count()
    results = []

    rows, cols, table = contingency(df, "weather_bucket", "severity")
    chi2, pval, dof, _ = stats.chi2_contingency(table)
    results.append({"test": "chi2_independence", "h0": "weather bucket and severity are independent",
                    "chi2": float(chi2), "dof": int(dof), "p_value": float(pval),
                    "cramers_v": cramers_v(chi2, table), "reject_h0": bool(pval < 0.05)})

    _, _, table = contingency(df, "is_night", "severe")
    chi2, pval, dof, _ = stats.chi2_contingency(table)
    results.append({"test": "chi2_night_vs_severe", "h0": "night and severe outcome are independent",
                    "chi2": float(chi2), "dof": int(dof), "p_value": float(pval),
                    "cramers_v": cramers_v(chi2, table), "reject_h0": bool(pval < 0.05)})

    fraction = min(1.0, a.sample_size / max(total, 1))
    sample = df.select("visibility_mi", "severity").dropna().sample(fraction=fraction, seed=3).toPandas()
    vis, sev = sample["visibility_mi"].to_numpy(), sample["severity"].to_numpy()
    rho, pval = stats.spearmanr(vis, sev)
    results.append({"test": "spearman_visibility_severity", "h0": "no monotonic relation",
                    "rho": float(rho), "p_value": float(pval), "n": int(len(vis)),
                    "reject_h0": bool(pval < 0.05)})
    groups = [vis[sev == s] for s in np.unique(sev) if (sev == s).sum() > 5]
    h, pval = stats.kruskal(*groups)
    results.append({"test": "kruskal_visibility_by_severity",
                    "h0": "visibility distribution equal across severity levels",
                    "h": float(h), "p_value": float(pval), "reject_h0": bool(pval < 0.05),
                    "medians": {str(int(s)): float(np.median(vis[sev == s])) for s in np.unique(sev)}})

    overall = df.agg(F.avg("severe")).first()[0]
    wr = df.groupBy("weather_bucket").agg(F.count("*").alias("n"), F.avg("severe").alias("severe_ratio")).collect()
    weather_risk = []
    for r in wr:
        k = r["severe_ratio"] * r["n"]
        # Normal-approximation (Wald) 95% CI; n is large for every bucket.
        z = stats.norm.ppf(0.975)
        p_hat, n = r["severe_ratio"], r["n"]
        half = z * np.sqrt(p_hat * (1 - p_hat) / n) if n else 0
        weather_risk.append({"bucket": r["weather_bucket"], "n": int(n), "severe": int(round(k)),
                             "severe_ratio": round(p_hat, 4), "ci95": [round(max(0, p_hat - half), 4),
                                                                        round(min(1, p_hat + half), 4)],
                             "relative_risk": round(p_hat / overall, 3) if overall else 1.0})

    now = datetime.now(timezone.utc)
    db = mongo_db()
    with mlflow_run("stats-tests") as log:
        for t in results:
            log.metrics(**{f"{t['test']}_p": t["p_value"]})
        log.json("stats_tests.json", results)
        log.json("weather_risk.json", weather_risk)
        db.stats_tests.delete_many({})
        db.stats_tests.insert_many([{**t, "created_at": now, "mlflow_run_id": log.run_id} for t in results])
        for w in weather_risk:
            db.weather_risk.update_one({"bucket": w["bucket"]}, {"$set": {**w, "updated_at": now}}, upsert=True)
        db.meta.update_one({"_id": "overall"}, {"$set": {"severe_ratio": overall, "accidents": total,
                                                          "updated_at": now}}, upsert=True)

    for t in results:
        print(f"[argus] {t['test']}: p={t['p_value']:.3g} reject_h0={t['reject_h0']}")
    for w in sorted(weather_risk, key=lambda x: -x["relative_risk"]):
        print(f"[argus] {w['bucket']:<13} RR={w['relative_risk']:.2f} n={w['n']:,}")
    spark.stop()


if __name__ == "__main__":
    main()
