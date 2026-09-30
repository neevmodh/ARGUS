import pytest

from argus.weather import BUCKETS, bucket, wmo_to_bucket


@pytest.mark.parametrize(
    "condition, expected",
    [
        ("Light Rain with Thunder", "Thunderstorm"),
        ("T-Storm", "Thunderstorm"),
        ("Freezing Rain", "Snow"),
        ("Wintry Mix", "Snow"),
        ("Heavy Rain", "Rain"),
        ("Light Drizzle", "Rain"),
        ("Haze", "Fog"),
        ("Mostly Cloudy", "Cloudy"),
        ("Overcast", "Cloudy"),
        ("Fair", "Clear"),
        ("Volcanic Ash", "Other"),
        ("", "Unknown"),
        (None, "Unknown"),
    ],
)
def test_bucket_matches_java_rules(condition, expected):
    # Same cases as mapreduce/src/test/.../ParsingTest.java
    assert bucket(condition) == expected


@pytest.mark.parametrize("code, expected", [(0, "Clear"), (3, "Cloudy"), (45, "Fog"), (61, "Rain"),
                                            (66, "Snow"), (75, "Snow"), (81, "Rain"), (95, "Thunderstorm"),
                                            (None, "Unknown")])
def test_wmo_codes(code, expected):
    assert wmo_to_bucket(code) == expected


def test_all_wmo_codes_land_in_known_buckets():
    assert {wmo_to_bucket(c) for c in range(100)} <= set(BUCKETS)
