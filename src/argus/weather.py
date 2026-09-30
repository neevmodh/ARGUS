"""Collapse ~140 free-text weather conditions into a handful of buckets.

The same ordered rules are mirrored in mapreduce/.../WeatherBucket.java so that
MapReduce, Spark and the live stream all agree. First match wins.
"""

BUCKET_RULES = [
    ("Thunderstorm", ("thunder", "t-storm", "tornado", "squall")),
    ("Snow", ("snow", "sleet", "ice", "freezing", "hail", "wintry")),
    ("Rain", ("rain", "drizzle", "shower")),
    ("Fog", ("fog", "mist", "haze", "smoke", "dust", "sand")),
    ("Cloudy", ("cloud", "overcast")),
    ("Clear", ("clear", "fair")),
]

BUCKETS = [b for b, _ in BUCKET_RULES] + ["Other", "Unknown"]


def bucket(condition: str | None) -> str:
    if not condition or not condition.strip():
        return "Unknown"
    text = condition.lower()
    for name, keywords in BUCKET_RULES:
        if any(k in text for k in keywords):
            return name
    return "Other"


def bucket_col(col):
    """Spark Column version of `bucket` (native expression, no Python UDF)."""
    from pyspark.sql import functions as F

    lowered = F.lower(F.trim(col))
    expr = F.when(col.isNull() | (F.trim(col) == ""), F.lit("Unknown"))
    for name, keywords in BUCKET_RULES:
        cond = None
        for k in keywords:
            c = lowered.contains(k)
            cond = c if cond is None else (cond | c)
        expr = expr.when(cond, F.lit(name))
    return expr.otherwise(F.lit("Other"))


def wmo_to_bucket(code: int | None) -> str:
    """Map an Open-Meteo WMO weather code to an ARGUS bucket."""
    if code is None:
        return "Unknown"
    if code in (0, 1):
        return "Clear"
    if code in (2, 3):
        return "Cloudy"
    if code in (45, 48):
        return "Fog"
    if code in (56, 57, 66, 67) or 71 <= code <= 77 or code in (85, 86):
        return "Snow"
    if 51 <= code <= 55 or 61 <= code <= 65 or 80 <= code <= 82:
        return "Rain"
    if 95 <= code <= 99:
        return "Thunderstorm"
    return "Other"
