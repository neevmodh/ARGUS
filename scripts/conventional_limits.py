"""Unit I: what a single machine + conventional tools cost on this data.

Loads the CSV into SQLite and pandas, reporting time and peak memory, and
extrapolates to the full 7.7M-row dataset if a smaller file is given.
"""

import csv
import os
import resource
import sqlite3
import sys
import time

FULL_ROWS = 7_728_394


def peak_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def main(path):
    size_mb = os.path.getsize(path) / 1e6
    print(f"file: {path} ({size_mb:,.0f} MB)")

    t0 = time.perf_counter()
    con = sqlite3.connect("/tmp/argus_conventional.db")
    con.execute("DROP TABLE IF EXISTS accidents")
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        con.execute(f"CREATE TABLE accidents ({', '.join(f'c{i} TEXT' for i in range(len(header)))})")
        rows = 0
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) == 50_000:
                con.executemany(f"INSERT INTO accidents VALUES ({','.join('?' * len(header))})", batch)
                rows += len(batch)
                batch.clear()
        if batch:
            con.executemany(f"INSERT INTO accidents VALUES ({','.join('?' * len(header))})", batch)
            rows += len(batch)
    con.commit()
    load_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    con.execute("SELECT c14, COUNT(*), AVG(CAST(c2 AS INT)) FROM accidents GROUP BY c14").fetchall()
    query_s = time.perf_counter() - t0
    print(f"SQLite: load {rows:,} rows in {load_s:.1f}s, group-by query {query_s:.1f}s (single core, single disk)")

    import pandas as pd

    t0 = time.perf_counter()
    df = pd.read_csv(path, low_memory=False)
    pd_s = time.perf_counter() - t0
    mem_mb = df.memory_usage(deep=True).sum() / 1e6
    print(f"pandas: read in {pd_s:.1f}s, DataFrame holds {mem_mb:,.0f} MB in RAM, process peak {peak_mb():,.0f} MB")

    if rows < FULL_ROWS:
        scale = FULL_ROWS / rows
        print(f"\nExtrapolated to the full dataset ({FULL_ROWS:,} rows, x{scale:.1f}):")
        print(f"  SQLite load  ~{load_s * scale / 60:.1f} min   pandas RAM ~{mem_mb * scale / 1e3:.1f} GB")
    print("\nNo parallelism, no fault tolerance, and memory bound by one machine - the case for HDFS + Spark.")


if __name__ == "__main__":
    main(sys.argv[1])
