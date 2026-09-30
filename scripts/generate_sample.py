#!/usr/bin/env python3
"""Write a synthetic US-Accidents-shaped CSV. Stdlib only, runs on the host."""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from argus.sample_data import write_csv  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/sample/us_accidents_sample.csv")
    a = p.parse_args()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    write_csv(a.out, a.rows, a.seed)
    print(f"wrote {a.rows:,} synthetic rows -> {a.out} ({os.path.getsize(a.out) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
