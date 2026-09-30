-- Same query as MapReduce `severity-stats -Dargus.groupBy=state` and jobs/benchmark/spark_engines.py
SELECT state, COUNT(*) AS n, AVG(CAST(severity AS INT)) AS avg_sev,
       AVG(CASE WHEN CAST(severity AS INT) >= 3 THEN 1.0 ELSE 0.0 END) AS severe
FROM argus.accidents_raw
GROUP BY state;
