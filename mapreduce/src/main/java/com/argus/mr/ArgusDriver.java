package com.argus.mr;

import org.apache.hadoop.util.ProgramDriver;

/** Entry point: hadoop jar argus-mr.jar <program> [-D ...] <in> <out> */
public final class ArgusDriver {

    private ArgusDriver() {}

    public static void main(String[] args) throws Throwable {
        ProgramDriver pd = new ProgramDriver();
        pd.addClass("state-hour", StateHourCount.class, "Accidents per state x hour-of-day");
        pd.addClass("severity-stats", SeverityStats.class, "Count / avg severity / severe ratio by -Dargus.groupBy");
        pd.addClass("top-streets", TopDangerousStreets.class, "Top-N dangerous streets (2 chained jobs)");
        System.exit(pd.run(args));
    }
}
