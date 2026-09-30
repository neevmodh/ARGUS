package com.argus.mr;

import java.util.ArrayList;
import java.util.List;

/** RFC-4180 style line splitter; the Description column contains quoted commas. */
public final class CsvParser {

    private CsvParser() {}

    public static String[] parse(String line) {
        List<String> out = new ArrayList<>(48);
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                out.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        out.add(field.toString());
        return out.toArray(new String[0]);
    }
}
