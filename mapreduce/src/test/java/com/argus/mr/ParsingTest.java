package com.argus.mr;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class ParsingTest {

    @Test
    void splitsQuotedCommasAndEscapedQuotes() {
        assertArrayEquals(new String[] {"a", "b, c", "say \"hi\"", ""},
                CsvParser.parse("a,\"b, c\",\"say \"\"hi\"\"\","));
    }

    @Test
    void bucketsWeatherLikePython() {
        assertEquals("Thunderstorm", WeatherBucket.of("Light Rain with Thunder"));
        assertEquals("Snow", WeatherBucket.of("Freezing Rain"));
        assertEquals("Rain", WeatherBucket.of("Heavy Rain"));
        assertEquals("Fog", WeatherBucket.of("Haze"));
        assertEquals("Cloudy", WeatherBucket.of("Mostly Cloudy"));
        assertEquals("Clear", WeatherBucket.of("Fair"));
        assertEquals("Other", WeatherBucket.of("Volcanic Ash Plume"));
        assertEquals("Unknown", WeatherBucket.of(""));
    }

    @Test
    void parsesAccidentRow() {
        String[] cols = new String[Accident.COLUMN_COUNT];
        Collections.nCopies(cols.length, "").toArray(cols);
        cols[Accident.SEVERITY] = "3";
        cols[Accident.START_TIME] = "2021-06-01 17:42:00.000000000";
        cols[Accident.STREET] = "I-95 N";
        cols[Accident.CITY] = "Miami";
        cols[Accident.STATE] = "FL";
        cols[Accident.WEATHER_CONDITION] = "T-Storm";
        cols[10] = "\"Crash on I-95, lanes blocked\"";

        Accident a = Accident.parse(String.join(",", cols));
        assertNotNull(a);
        assertEquals(3, a.severity);
        assertEquals(17, a.hour);
        assertEquals("FL", a.state);
        assertEquals("Thunderstorm", a.weatherBucket);
    }

    @Test
    void rejectsMalformedRows() {
        assertNull(Accident.parse("too,few,columns"));
        assertTrue(Accident.isHeader("ID,Source,Severity"));
    }
}
