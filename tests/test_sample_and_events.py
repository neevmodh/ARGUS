import csv
import io
from collections import Counter
from datetime import datetime, timezone

from argus.events import build_event
from argus.sample_data import generate_rows
from argus.schema import RAW_COLUMNS, ROAD_FLAGS


def sample_rows(n=3000):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(RAW_COLUMNS)
    w.writerows(generate_rows(n, seed=1))
    buf.seek(0)
    return list(csv.DictReader(buf))


def test_sample_matches_dataset_schema():
    rows = sample_rows()
    assert list(rows[0].keys()) == RAW_COLUMNS
    assert len(RAW_COLUMNS) == 46
    assert all(r["ID"].startswith("S-") for r in rows)
    assert "," in rows[0]["Description"]  # exercises CSV quoting downstream


def test_sample_has_learnable_signal():
    rows = sample_rows(8000)
    severe = lambda rs: sum(r["Severity"] in ("3", "4") for r in rs) / len(rs)  # noqa: E731
    night = [r for r in rows if r["Sunrise_Sunset"] == "Night"]
    day = [r for r in rows if r["Sunrise_Sunset"] == "Day"]
    assert severe(night) > severe(day)
    assert Counter(r["Severity"] for r in rows).most_common(1)[0][0] == "2"


def test_build_event_uses_local_time_and_row_weather():
    row = sample_rows(1)[0]
    row["Timezone"] = "US/Eastern"
    now = datetime(2026, 7, 1, 23, 30, tzinfo=timezone.utc)
    e = build_event(row, now_utc=now)
    assert e["local_time"] == "2026-07-01 19:30:00"  # EDT = UTC-4
    assert e["is_night"] == 1
    assert e["weather_source"] == "historic"
    assert set(ROAD_FLAGS) <= e.keys()


def test_build_event_overlays_live_weather():
    row = sample_rows(1)[0]
    live = {"temperature_f": 30.0, "humidity": 90, "visibility_mi": 0.8, "wind_speed_mph": 12.0,
            "precipitation_in": 0.1, "is_day": 1, "bucket": "Snow"}
    e = build_event(row, live_weather=live)
    assert e["weather_bucket"] == "Snow"
    assert e["is_night"] == 0
    assert e["weather_source"] == "open-meteo"
