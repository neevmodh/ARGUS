package com.argus.mr;

/** The handful of US Accidents columns the MapReduce jobs need. */
public final class Accident {

    static final int COLUMN_COUNT = 46;
    static final int SEVERITY = 2;
    static final int START_TIME = 3;
    static final int STREET = 11;
    static final int CITY = 12;
    static final int STATE = 14;
    static final int WEATHER_CONDITION = 28;

    public enum Counters { RECORDS_OK, RECORDS_MALFORMED, HEADER_SKIPPED }

    public final int severity;
    public final int hour;
    public final String street;
    public final String city;
    public final String state;
    public final String weatherBucket;

    private Accident(int severity, int hour, String street, String city, String state, String weatherBucket) {
        this.severity = severity;
        this.hour = hour;
        this.street = street;
        this.city = city;
        this.state = state;
        this.weatherBucket = weatherBucket;
    }

    public static boolean isHeader(String line) {
        return line.startsWith("ID,");
    }

    /** Returns null for malformed rows; callers count them via {@link Counters}. */
    public static Accident parse(String line) {
        String[] f = CsvParser.parse(line);
        if (f.length != COLUMN_COUNT) {
            return null;
        }
        try {
            int severity = Integer.parseInt(f[SEVERITY].trim());
            String start = f[START_TIME].trim();
            int hour = Integer.parseInt(start.substring(11, 13));
            String state = f[STATE].trim();
            if (state.isEmpty() || severity < 1 || severity > 4 || hour < 0 || hour > 23) {
                return null;
            }
            return new Accident(severity, hour, f[STREET].trim(), f[CITY].trim(), state,
                    WeatherBucket.of(f[WEATHER_CONDITION]));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            return null;
        }
    }
}
