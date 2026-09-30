"""Write continuously to the replica set and report the unavailability window."""

import argparse
import time

from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError

from argus import config


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=45)
    p.add_argument("--interval", type=float, default=0.1)
    a = p.parse_args()

    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000, retryWrites=True)
    coll = client[config.MONGO_DB].get_collection(
        "failover_probe", write_concern=WriteConcern(w="majority", wtimeout=3000))
    coll.drop()

    ok = failed = 0
    last_ok = time.time()
    worst_gap = 0.0
    end = time.time() + a.seconds
    seq = 0
    while time.time() < end:
        seq += 1
        try:
            coll.insert_one({"seq": seq, "ts": time.time()})
            now = time.time()
            gap = now - last_ok
            if gap > 1.0:
                print(f"[writer] writes resumed after {gap:.1f}s outage (seq {seq})", flush=True)
            worst_gap = max(worst_gap, gap)
            last_ok = now
            ok += 1
        except PyMongoError as exc:
            failed += 1
            print(f"[writer] seq {seq} failed: {type(exc).__name__}", flush=True)
        time.sleep(a.interval)

    stored = coll.count_documents({})
    print(f"[writer] attempts={seq} ok={ok} failed={failed} stored={stored} "
          f"longest write gap={worst_gap:.1f}s  acknowledged-but-lost={ok - stored}", flush=True)


if __name__ == "__main__":
    main()
