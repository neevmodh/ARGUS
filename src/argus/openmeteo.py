"""Tiny cached client for the free Open-Meteo current-weather API (no key needed)."""

import time

import requests

from argus.weather import wmo_to_bucket

URL = "https://api.open-meteo.com/v1/forecast"
FIELDS = "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m,visibility,is_day"


class OpenMeteo:
    def __init__(self, cell_deg: float = 0.25, ttl_s: int = 900, timeout_s: float = 5.0):
        self.cell_deg = cell_deg
        self.ttl_s = ttl_s
        self.timeout_s = timeout_s
        self._cache: dict[tuple, tuple[float, dict | None]] = {}

    def current(self, lat: float, lng: float) -> dict | None:
        """Current weather near (lat, lng); cached per grid cell. None on any failure."""
        key = (round(lat / self.cell_deg), round(lng / self.cell_deg))
        hit = self._cache.get(key)
        if hit and time.time() - hit[0] < self.ttl_s:
            return hit[1]
        result = None
        try:
            r = requests.get(URL, timeout=self.timeout_s, params={
                "latitude": round(lat, 3), "longitude": round(lng, 3), "current": FIELDS,
                "temperature_unit": "fahrenheit", "wind_speed_unit": "mph", "precipitation_unit": "inch",
            })
            r.raise_for_status()
            c = r.json()["current"]
            vis_m = c.get("visibility")
            result = {
                "temperature_f": c.get("temperature_2m"),
                "humidity": c.get("relative_humidity_2m"),
                "precipitation_in": c.get("precipitation") or 0.0,
                "wind_speed_mph": c.get("wind_speed_10m"),
                "visibility_mi": round(vis_m / 1609.34, 2) if vis_m is not None else None,
                "is_day": c.get("is_day", 1),
                "weather_code": c.get("weather_code"),
                "bucket": wmo_to_bucket(c.get("weather_code")),
            }
        except (requests.RequestException, KeyError, ValueError) as exc:
            print(f"[open-meteo] {type(exc).__name__}: falling back to historic weather")
        self._cache[key] = (time.time(), result)
        return result
