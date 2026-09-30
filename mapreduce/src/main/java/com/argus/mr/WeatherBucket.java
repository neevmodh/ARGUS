package com.argus.mr;

import java.util.Locale;

/** Mirrors src/argus/weather.py BUCKET_RULES. Keep both in sync; first match wins. */
public final class WeatherBucket {

    private static final String[][] RULES = {
        {"Thunderstorm", "thunder", "t-storm", "tornado", "squall"},
        {"Snow", "snow", "sleet", "ice", "freezing", "hail", "wintry"},
        {"Rain", "rain", "drizzle", "shower"},
        {"Fog", "fog", "mist", "haze", "smoke", "dust", "sand"},
        {"Cloudy", "cloud", "overcast"},
        {"Clear", "clear", "fair"},
    };

    private WeatherBucket() {}

    public static String of(String condition) {
        if (condition == null || condition.trim().isEmpty()) {
            return "Unknown";
        }
        String text = condition.toLowerCase(Locale.ROOT);
        for (String[] rule : RULES) {
            for (int i = 1; i < rule.length; i++) {
                if (text.contains(rule[i])) {
                    return rule[0];
                }
            }
        }
        return "Other";
    }
}
