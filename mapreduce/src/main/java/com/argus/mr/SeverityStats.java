package com.argus.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Count, average severity and severe (>=3) ratio grouped by a configurable key:
 * -Dargus.groupBy=weather|state|city|hour  (default: weather).
 * Shows: custom Writable value type, same class as combiner and reducer.
 */
public class SeverityStats extends Configured implements Tool {

    public static final String GROUP_BY = "argus.groupBy";

    public static class GroupMapper extends Mapper<LongWritable, Text, Text, SeverityWritable> {
        private final Text outKey = new Text();
        private String groupBy;

        @Override
        protected void setup(Context ctx) {
            groupBy = ctx.getConfiguration().get(GROUP_BY, "weather");
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (Accident.isHeader(s)) {
                ctx.getCounter(Accident.Counters.HEADER_SKIPPED).increment(1);
                return;
            }
            Accident a = Accident.parse(s);
            if (a == null) {
                ctx.getCounter(Accident.Counters.RECORDS_MALFORMED).increment(1);
                return;
            }
            ctx.getCounter(Accident.Counters.RECORDS_OK).increment(1);
            switch (groupBy) {
                case "state": outKey.set(a.state); break;
                case "city": outKey.set(a.city + ", " + a.state); break;
                case "hour": outKey.set(String.format("%02d", a.hour)); break;
                default: outKey.set(a.weatherBucket);
            }
            ctx.write(outKey, SeverityWritable.of(a.severity));
        }
    }

    public static class MergeReducer extends Reducer<Text, SeverityWritable, Text, SeverityWritable> {
        private final SeverityWritable acc = new SeverityWritable();

        @Override
        protected void reduce(Text key, Iterable<SeverityWritable> values, Context ctx)
                throws IOException, InterruptedException {
            acc.reset();
            for (SeverityWritable v : values) {
                acc.merge(v);
            }
            ctx.write(key, acc);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            return JobUtil.usage("severity-stats", "   (-Dargus.groupBy=weather|state|city|hour)");
        }
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        JobUtil.prepareOutput(getConf(), out);

        String groupBy = getConf().get(GROUP_BY, "weather");
        Job job = Job.getInstance(getConf(), "ARGUS: severity stats by " + groupBy);
        job.setJarByClass(SeverityStats.class);
        job.setMapperClass(GroupMapper.class);
        job.setCombinerClass(MergeReducer.class);
        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SeverityWritable.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SeverityStats(), args));
    }
}
