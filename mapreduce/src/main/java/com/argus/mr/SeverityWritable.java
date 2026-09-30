package com.argus.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.Writable;

/** Custom Hadoop type: (count, severity sum, severe count) - associative, so it combines. */
public class SeverityWritable implements Writable {

    private long count;
    private long severitySum;
    private long severeCount;

    public SeverityWritable() {}

    public SeverityWritable(long count, long severitySum, long severeCount) {
        this.count = count;
        this.severitySum = severitySum;
        this.severeCount = severeCount;
    }

    public static SeverityWritable of(int severity) {
        return new SeverityWritable(1, severity, severity >= 3 ? 1 : 0);
    }

    public void reset() {
        count = 0;
        severitySum = 0;
        severeCount = 0;
    }

    public void merge(SeverityWritable o) {
        count += o.count;
        severitySum += o.severitySum;
        severeCount += o.severeCount;
    }

    public long getCount() {
        return count;
    }

    public long getSeveritySum() {
        return severitySum;
    }

    public double avgSeverity() {
        return count == 0 ? 0 : (double) severitySum / count;
    }

    public double severeRatio() {
        return count == 0 ? 0 : (double) severeCount / count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(severitySum);
        out.writeLong(severeCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        severitySum = in.readLong();
        severeCount = in.readLong();
    }

    /** TextOutputFormat line: count, avg severity, severe ratio. */
    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%d\t%.4f\t%.4f", count, avgSeverity(), severeRatio());
    }
}
