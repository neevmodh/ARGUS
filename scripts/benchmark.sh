#!/usr/bin/env bash
# One query - accidents, avg severity, severe ratio per state - on every engine.
# Writes results/benchmark.csv and results/benchmark.png.
set -euo pipefail
cd "$(dirname "$0")/.."

C="docker compose"
OUT=results/benchmark.csv
mkdir -p results
echo "engine,seconds" > "$OUT"

running() { [ -n "$($C ps -q --status running "$1" 2>/dev/null)" ]; }

echo "==> MapReduce on YARN"
secs=$($C exec -T namenode bash -c '
  s=$(date +%s%N)
  hadoop jar /opt/argus/mapreduce/target/argus-mr.jar severity-stats -Dargus.groupBy=state \
    /argus/raw/accidents /argus/bench/mr_by_state >/tmp/bench_mr.log 2>&1
  e=$(date +%s%N); echo $(( (e-s)/1000000 ))')
echo "mapreduce,$(awk "BEGIN{printf \"%.3f\", $secs/1000}")" >> "$OUT"

if running hiveserver2; then
  echo "==> Hive (Tez local) on raw CSV"
  secs=$($C exec -T hiveserver2 bash -c '
    s=$(date +%s%N)
    beeline -u jdbc:hive2://localhost:10000/ --silent=true -f /opt/argus-hive/queries/benchmark.sql >/dev/null 2>&1
    e=$(date +%s%N); echo $(( (e-s)/1000000 ))')
  echo "hive,$(awk "BEGIN{printf \"%.3f\", $secs/1000}")" >> "$OUT"
else
  echo "(skipping Hive - start it with: make up-hive && make hive-init)"
fi

echo "==> Spark: RDD / DataFrame / SQL on CSV, DataFrame on Parquet"
$C exec -T spark-master argus-submit jobs/benchmark/spark_engines.py 2>/dev/null \
  | awk -F, '/^BENCH,/ {print $2","$3}' >> "$OUT"

echo
column -s, -t "$OUT"
$C run --rm --no-deps dashboard python scripts/plot_benchmark.py results/benchmark.csv results/benchmark.png
