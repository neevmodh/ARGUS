"""Synthetic rows in the exact US Accidents schema.

Lets the full pipeline run (and CI test it) before the 3 GB Kaggle file is
downloaded. Signal is planted on purpose - hotspot clusters, rush hours, and
weather/night/junction effects on severity - so every ML stage has something
real to find. IDs are prefixed `S-` so synthetic rows are never mistaken for
real ones.
"""

import csv
import math
import random
from datetime import datetime, timedelta

from argus.schema import RAW_COLUMNS

CITIES = [
    # city, county, state, lat, lng, timezone, airport
    ("Los Angeles", "Los Angeles", "CA", 34.05, -118.24, "US/Pacific", "KCQT"),
    ("San Diego", "San Diego", "CA", 32.72, -117.16, "US/Pacific", "KSAN"),
    ("Sacramento", "Sacramento", "CA", 38.58, -121.49, "US/Pacific", "KSAC"),
    ("Houston", "Harris", "TX", 29.76, -95.37, "US/Central", "KHOU"),
    ("Dallas", "Dallas", "TX", 32.78, -96.80, "US/Central", "KDAL"),
    ("Austin", "Travis", "TX", 30.27, -97.74, "US/Central", "KAUS"),
    ("Miami", "Miami-Dade", "FL", 25.76, -80.19, "US/Eastern", "KMIA"),
    ("Orlando", "Orange", "FL", 28.54, -81.38, "US/Eastern", "KORL"),
    ("Charlotte", "Mecklenburg", "NC", 35.23, -80.84, "US/Eastern", "KCLT"),
    ("Raleigh", "Wake", "NC", 35.78, -78.64, "US/Eastern", "KRDU"),
    ("Atlanta", "Fulton", "GA", 33.75, -84.39, "US/Eastern", "KATL"),
    ("New York", "New York", "NY", 40.71, -74.01, "US/Eastern", "KNYC"),
    ("Philadelphia", "Philadelphia", "PA", 39.95, -75.17, "US/Eastern", "KPHL"),
    ("Chicago", "Cook", "IL", 41.88, -87.63, "US/Central", "KMDW"),
    ("Minneapolis", "Hennepin", "MN", 44.98, -93.27, "US/Central", "KMSP"),
    ("Denver", "Denver", "CO", 39.74, -104.99, "US/Mountain", "KDEN"),
    ("Phoenix", "Maricopa", "AZ", 33.45, -112.07, "US/Mountain", "KPHX"),
    ("Seattle", "King", "WA", 47.61, -122.33, "US/Pacific", "KSEA"),
    ("Portland", "Multnomah", "OR", 45.52, -122.68, "US/Pacific", "KPDX"),
    ("Nashville", "Davidson", "TN", 36.16, -86.78, "US/Central", "KBNA"),
    ("Columbus", "Franklin", "OH", 39.96, -83.00, "US/Eastern", "KCMH"),
    ("Detroit", "Wayne", "MI", 42.33, -83.05, "US/Eastern", "KDET"),
    ("Richmond", "Richmond City", "VA", 37.54, -77.44, "US/Eastern", "KRIC"),
    ("Baton Rouge", "East Baton Rouge", "LA", 30.45, -91.19, "US/Central", "KBTR"),
    ("Salt Lake City", "Salt Lake", "UT", 40.76, -111.89, "US/Mountain", "KSLC"),
]
CITY_WEIGHTS = [14, 5, 5, 8, 6, 5, 9, 7, 5, 4, 5, 6, 4, 5, 4, 4, 5, 4, 3, 4, 3, 3, 3, 3, 2]

NORTHERN = {"NY", "PA", "IL", "MN", "CO", "WA", "OR", "OH", "MI", "UT"}

WEATHER = {
    "Fair": 30, "Clear": 12, "Mostly Cloudy": 10, "Cloudy": 9, "Partly Cloudy": 8,
    "Overcast": 6, "Light Rain": 6, "Rain": 3, "Heavy Rain": 1, "Fog": 2, "Haze": 2,
    "Light Snow": 2, "Snow": 1, "Thunderstorm": 1, "T-Storm": 1, "Light Drizzle": 1,
    "Wintry Mix": 0.5, "Smoke": 0.5,
}

STREET_TEMPLATES = ["I-{a}", "US-{b}", "Main St", "Broadway", "{city} Blvd", "Oak Ave",
                    "Market St", "Highway {c}", "Park Rd", "Lincoln Ave"]

HOUR_WEIGHTS = [1, 1, 1, 1, 1.5, 3, 6, 9, 8, 5, 4, 4, 4.5, 5, 6, 8, 9.5, 9, 6, 4, 3, 2.5, 2, 1.5]

FLAG_P = {
    "Amenity": 0.012, "Bump": 0.0005, "Crossing": 0.11, "Give_Way": 0.005,
    "Junction": 0.07, "No_Exit": 0.002, "Railway": 0.009, "Roundabout": 0.0001,
    "Station": 0.026, "Stop": 0.028, "Traffic_Calming": 0.001, "Traffic_Signal": 0.15,
}


def _city_layout(rng: random.Random):
    """Per city: street names and 3-6 hotspot centres (some flagged dangerous)."""
    layout = {}
    for city, _county, _state, lat, lng, _tz, _airport in CITIES:
        streets = [t.format(a=rng.choice([5, 10, 35, 40, 75, 80, 95]), b=rng.randint(1, 99),
                            c=rng.randint(1, 400), city=city.split()[0]) for t in STREET_TEMPLATES]
        spots = []
        for _ in range(rng.randint(3, 6)):
            spots.append({
                "lat": lat + rng.uniform(-0.12, 0.12),
                "lng": lng + rng.uniform(-0.12, 0.12),
                "street": rng.choice(streets[:4] + [streets[7]]),
                "danger": rng.random() < 0.35,
            })
        layout[city] = {"streets": streets, "spots": spots}
    return layout


def _weather_for(rng, month, state):
    weights = dict(WEATHER)
    winter = month in (12, 1, 2)
    if state in NORTHERN and winter:
        for w in ("Light Snow", "Snow", "Wintry Mix"):
            weights[w] *= 6
    elif state not in NORTHERN:
        for w in ("Light Snow", "Snow", "Wintry Mix"):
            weights[w] *= 0.05
    if month in (6, 7, 8) and state in {"FL", "TX", "LA", "GA"}:
        for w in ("Thunderstorm", "T-Storm", "Rain", "Light Rain"):
            weights[w] *= 3
    names = list(weights)
    return rng.choices(names, weights=[weights[n] for n in names])[0]


def _bool(v: bool) -> str:
    return "True" if v else "False"


def generate_rows(n: int, seed: int = 42):
    rng = random.Random(seed)
    layout = _city_layout(rng)
    start = datetime(2016, 2, 1)
    span_days = (datetime(2023, 3, 31) - start).days

    for i in range(1, n + 1):
        city, county, state, clat, clng, tz, airport = rng.choices(CITIES, weights=CITY_WEIGHTS)[0]
        city_layout = layout[city]

        if rng.random() < 0.7:
            spot = rng.choice(city_layout["spots"])
            lat = rng.gauss(spot["lat"], 0.008)
            lng = rng.gauss(spot["lng"], 0.008)
            street, danger = spot["street"], spot["danger"]
        else:
            lat = clat + rng.uniform(-0.2, 0.2)
            lng = clng + rng.uniform(-0.2, 0.2)
            street, danger = rng.choice(city_layout["streets"]), False

        day = start + timedelta(days=rng.randrange(span_days))
        hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        ts = day.replace(hour=hour, minute=rng.randrange(60), second=rng.randrange(60))
        end = ts + timedelta(minutes=rng.randint(15, 360))
        night = hour < 6 or hour >= 19

        weather = _weather_for(rng, ts.month, state)
        wl = weather.lower()
        snowy = any(k in wl for k in ("snow", "wintry"))
        stormy = "storm" in wl
        rainy = any(k in wl for k in ("rain", "drizzle"))
        foggy = any(k in wl for k in ("fog", "haze", "smoke"))

        base_temp = 50 + 25 * math.sin((ts.month - 4) / 12 * 2 * math.pi) + (-12 if state in NORTHERN else 8)
        temp = round(rng.gauss(base_temp - (4 if night else 0) - (15 if snowy else 0), 6), 1)
        if snowy:
            temp = min(temp, 33.0)
        visibility = 10.0
        if foggy:
            visibility = round(rng.uniform(0.2, 3.0), 1)
        elif snowy:
            visibility = round(rng.uniform(0.5, 5.0), 1)
        elif rainy or stormy:
            visibility = round(rng.uniform(1.5, 8.0), 1)
        humidity = round(min(100, max(10, rng.gauss(85 if (rainy or foggy or snowy) else 60, 12))))
        wind = round(max(0.0, rng.gauss(18 if stormy else 8, 5)), 1)
        precip = round(rng.uniform(0.01, 0.4), 2) if (rainy or stormy or snowy) else 0.0

        flags = {k: rng.random() < p for k, p in FLAG_P.items()}

        score = 0.0
        score += 0.9 if (snowy or stormy) else 0.5 if (rainy or foggy) else 0.0
        score += 0.45 if night else 0.0
        score += 0.4 if visibility < 2 else 0.0
        score += 0.35 if flags["Junction"] else 0.0
        score -= 0.35 if (flags["Traffic_Signal"] or flags["Crossing"]) else 0.0
        score += 0.6 if danger else 0.0
        p_high = min(0.85, 0.11 * math.exp(score))
        r = rng.random()
        if r < p_high:
            severity = 4 if rng.random() < 0.22 else 3
        elif r < p_high + 0.02:
            severity = 1
        else:
            severity = 2

        cross = rng.choice(city_layout["streets"])
        row = {
            "ID": f"S-{i}",
            "Source": "Synthetic",
            "Severity": severity,
            "Start_Time": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "End_Time": end.strftime("%Y-%m-%d %H:%M:%S"),
            "Start_Lat": round(lat, 6),
            "Start_Lng": round(lng, 6),
            "End_Lat": round(lat + rng.uniform(-0.003, 0.003), 6),
            "End_Lng": round(lng + rng.uniform(-0.003, 0.003), 6),
            "Distance(mi)": round(rng.expovariate(1.5), 3),
            "Description": f"Accident on {street} at {cross}, {city}. Expect delays, use caution.",
            "Street": street,
            "City": city,
            "County": county,
            "State": state,
            "Zipcode": f"{rng.randint(10000, 99999)}",
            "Country": "US",
            "Timezone": tz,
            "Airport_Code": airport,
            "Weather_Timestamp": ts.replace(minute=53, second=0).strftime("%Y-%m-%d %H:%M:%S"),
            "Temperature(F)": temp,
            "Wind_Chill(F)": round(temp - wind * 0.3, 1),
            "Humidity(%)": humidity,
            "Pressure(in)": round(rng.uniform(29.2, 30.4), 2),
            "Visibility(mi)": visibility,
            "Wind_Direction": rng.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW", "CALM", "VAR"]),
            "Wind_Speed(mph)": wind,
            "Precipitation(in)": precip,
            "Weather_Condition": weather,
            "Turning_Loop": "False",
            "Sunrise_Sunset": "Night" if night else "Day",
            "Civil_Twilight": "Night" if (hour < 6 or hour >= 20) else "Day",
            "Nautical_Twilight": "Night" if (hour < 5 or hour >= 20) else "Day",
            "Astronomical_Twilight": "Night" if (hour < 5 or hour >= 21) else "Day",
        }
        for k, v in flags.items():
            row[k] = _bool(v)
        yield [row[c] for c in RAW_COLUMNS]


def write_csv(path: str, n: int, seed: int = 42) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(RAW_COLUMNS)
        w.writerows(generate_rows(n, seed))
