"""Column layout of the Kaggle US Accidents dataset (March 2023 release, 46 columns)."""

RAW_COLUMNS = [
    "ID", "Source", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
    "End_Lat", "End_Lng", "Distance(mi)", "Description", "Street", "City", "County",
    "State", "Zipcode", "Country", "Timezone", "Airport_Code", "Weather_Timestamp",
    "Temperature(F)", "Wind_Chill(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
    "Wind_Direction", "Wind_Speed(mph)", "Precipitation(in)", "Weather_Condition",
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit", "Railway",
    "Roundabout", "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop",
    "Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight",
]

ROAD_FLAGS = [
    "amenity", "bump", "crossing", "give_way", "junction", "no_exit", "railway",
    "roundabout", "station", "stop", "traffic_calming", "traffic_signal",
]

RAW_TO_ROAD_FLAG = {c: c.lower() for c in RAW_COLUMNS if c.lower() in ROAD_FLAGS}

# Features known *before/at* the time of an accident. Distance and duration are
# excluded on purpose: they describe the accident's aftermath (label leakage).
NUMERIC_FEATURES = [
    "hour", "day_of_week", "month", "is_night", "lat", "lng",
    "temperature_f", "humidity", "visibility_mi", "wind_speed_mph", "precipitation_in",
] + ROAD_FLAGS

CATEGORICAL_FEATURES = ["weather_bucket"]
