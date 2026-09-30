"""Replay accidents into Kafka as live events, optionally with live Open-Meteo weather.

  python streaming/producer.py --source /data/sample/us_accidents_sample.csv --rate 20 --live-weather
"""

import argparse
import csv
import json
import signal
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from argus import config
from argus.events import build_event
from argus.openmeteo import OpenMeteo


def ensure_topics(bootstrap: str, topics: list[str], partitions: int = 3):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=15).topics
    missing = [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics if t not in existing]
    if not missing:
        return
    for topic, fut in admin.create_topics(missing).items():
        try:
            fut.result()
            print(f"[producer] created topic {topic}")
        except KafkaException as exc:
            print(f"[producer] topic {topic}: {exc}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="/data/sample/us_accidents_sample.csv")
    p.add_argument("--rate", type=float, default=20.0, help="events per second")
    p.add_argument("--limit", type=int, default=0, help="stop after N events (0 = run forever)")
    p.add_argument("--live-weather", action="store_true", help="overlay current Open-Meteo weather")
    a = p.parse_args()

    alerts_topic = f"{config.KAFKA_TOPIC.rsplit('.', 1)[0]}.alerts"
    ensure_topics(config.KAFKA_BOOTSTRAP, [config.KAFKA_TOPIC, alerts_topic])
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50, "acks": "all"})
    weather = OpenMeteo() if a.live_weather else None

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent, started, interval = 0, time.time(), 1.0 / a.rate
    while running:
        with open(a.source, newline="") as f:
            for row in csv.DictReader(f):
                if not running or (a.limit and sent >= a.limit):
                    running = False
                    break
                try:
                    lat, lng = float(row["Start_Lat"]), float(row["Start_Lng"])
                except (TypeError, ValueError):
                    continue
                live = weather.current(lat, lng) if weather else None
                event = build_event(row, live_weather=live)
                producer.produce(config.KAFKA_TOPIC, key=event["state"] or "", value=json.dumps(event))
                producer.poll(0)
                sent += 1
                if sent % 500 == 0:
                    print(f"[producer] {sent:,} events ({sent / (time.time() - started):.1f}/s)")
                time.sleep(interval)
    producer.flush(10)
    print(f"[producer] done: {sent:,} events sent to {config.KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
