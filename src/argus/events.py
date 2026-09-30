"""Turn a historical accident row into a live event (used by the Kafka producer)."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from argus.schema import RAW_TO_ROAD_FLAG
from argus.weather import bucket


def _num(v):
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def local_time(now_utc: datetime, tz_name: str | None) -> datetime:
    try:
        return now_utc.astimezone(ZoneInfo(tz_name)) if tz_name else now_utc
    except (ZoneInfoNotFoundError, ValueError):
        return now_utc


def build_event(row: dict, now_utc: datetime | None = None, live_weather: dict | None = None) -> dict:
    """Re-stamp a historical row to *now*; optionally overlay live Open-Meteo weather.

    `local_time` (the accident's own timezone) drives hour/day features, matching
    how the model was trained on the dataset's local Start_Time.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    local = local_time(now_utc, row.get("Timezone"))

    event = {
        "id": row["ID"],
        "event_time": now_utc.isoformat(),
        "local_time": local.strftime("%Y-%m-%d %H:%M:%S"),
        "lat": _num(row["Start_Lat"]),
        "lng": _num(row["Start_Lng"]),
        "city": row.get("City"),
        "state": row.get("State"),
        "street": row.get("Street"),
        "severity_actual": int(row["Severity"]) if row.get("Severity") else None,
    }
    for raw, flag in RAW_TO_ROAD_FLAG.items():
        event[flag] = 1 if row.get(raw) == "True" else 0

    if live_weather:
        event.update(
            temperature_f=live_weather.get("temperature_f"),
            humidity=live_weather.get("humidity"),
            visibility_mi=live_weather.get("visibility_mi"),
            wind_speed_mph=live_weather.get("wind_speed_mph"),
            precipitation_in=live_weather.get("precipitation_in"),
            weather_condition=live_weather["bucket"],
            weather_bucket=live_weather["bucket"],
            is_night=0 if live_weather.get("is_day", 1) else 1,
            weather_source="open-meteo",
        )
    else:
        event.update(
            temperature_f=_num(row.get("Temperature(F)")),
            humidity=_num(row.get("Humidity(%)")),
            visibility_mi=_num(row.get("Visibility(mi)")),
            wind_speed_mph=_num(row.get("Wind_Speed(mph)")),
            precipitation_in=_num(row.get("Precipitation(in)")) or 0.0,
            weather_condition=row.get("Weather_Condition"),
            weather_bucket=bucket(row.get("Weather_Condition")),
            is_night=1 if (local.hour < 6 or local.hour >= 19) else 0,
            weather_source="historic",
        )
    return event
