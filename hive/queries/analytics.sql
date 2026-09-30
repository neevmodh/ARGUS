USE argus;

-- 1. Most accident-prone cities with average severity
SELECT city, state, COUNT(*) AS accidents, ROUND(AVG(severity), 3) AS avg_severity
FROM accidents
GROUP BY city, state
ORDER BY accidents DESC
LIMIT 15;

-- 2. Partition pruning: only the FL partitions are scanned (compare EXPLAIN with/without WHERE)
SELECT year, COUNT(*) AS accidents
FROM accidents
WHERE state = 'FL'
GROUP BY year
ORDER BY year;

-- 3. Window function: top 3 dangerous hours per state
SELECT state, hour, accidents, rnk FROM (
  SELECT state, hour, COUNT(*) AS accidents,
         RANK() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rnk
  FROM accidents
  GROUP BY state, hour
) t
WHERE rnk <= 3
ORDER BY state, rnk
LIMIT 60;

-- 4. Severe-accident ratio by weather and light condition
SELECT weather_bucket,
       CASE WHEN is_night = 1 THEN 'Night' ELSE 'Day' END AS light,
       COUNT(*) AS accidents,
       ROUND(AVG(CASE WHEN severity >= 3 THEN 1.0 ELSE 0.0 END), 4) AS severe_ratio
FROM accidents
GROUP BY weather_bucket, CASE WHEN is_night = 1 THEN 'Night' ELSE 'Day' END
ORDER BY severe_ratio DESC;
