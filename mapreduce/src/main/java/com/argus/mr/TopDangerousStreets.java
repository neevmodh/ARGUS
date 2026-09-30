package com.argus.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Top-N most dangerous streets, scored by total severity (frequency x harm).
 * Two chained jobs: (1) aggregate per street -> SequenceFile, (2) global top-N.
 * Shows: job chaining, SequenceFile output/input formats, in-mapper top-N, single reducer.
 * Options: -Dargus.topN=25 -Dargus.minCount=20
 */
public class TopDangerousStreets extends Configured implements Tool {

    static final String TOP_N = "argus.topN";
    static final String MIN_COUNT = "argus.minCount";

    public static class StreetMapper extends Mapper<LongWritable, Text, Text, SeverityWritable> {
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (Accident.isHeader(s)) {
                return;
            }
            Accident a = Accident.parse(s);
            if (a == null) {
                ctx.getCounter(Accident.Counters.RECORDS_MALFORMED).increment(1);
                return;
            }
            if (a.street.isEmpty()) {
                return;
            }
            outKey.set(a.street + "|" + a.city + "|" + a.state);
            ctx.write(outKey, SeverityWritable.of(a.severity));
        }
    }

    /** Row kept in the top-N heaps: score first so the heap orders by it. */
    static final class Ranked implements Comparable<Ranked> {
        final long score;
        final String line;

        Ranked(long score, String line) {
            this.score = score;
            this.line = line;
        }

        @Override
        public int compareTo(Ranked o) {
            int c = Long.compare(score, o.score);
            return c != 0 ? c : line.compareTo(o.line);
        }
    }

    static void offer(PriorityQueue<Ranked> heap, Ranked r, int n) {
        heap.offer(r);
        if (heap.size() > n) {
            heap.poll();
        }
    }

    public static class TopNMapper extends Mapper<Text, SeverityWritable, NullWritable, Text> {
        private final PriorityQueue<Ranked> heap = new PriorityQueue<>();
        private int topN;
        private long minCount;

        @Override
        protected void setup(Context ctx) {
            Configuration c = ctx.getConfiguration();
            topN = c.getInt(TOP_N, 25);
            minCount = c.getLong(MIN_COUNT, 20);
        }

        @Override
        protected void map(Text street, SeverityWritable stats, Context ctx) {
            if (stats.getCount() < minCount) {
                return;
            }
            String line = String.format(Locale.ROOT, "%d\t%s\t%s",
                    stats.getSeveritySum(), stats, street);
            offer(heap, new Ranked(stats.getSeveritySum(), line), topN);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Ranked r : heap) {
                ctx.write(NullWritable.get(), new Text(r.line));
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable k, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            int topN = ctx.getConfiguration().getInt(TOP_N, 25);
            PriorityQueue<Ranked> heap = new PriorityQueue<>();
            for (Text v : values) {
                String line = v.toString();
                long score = Long.parseLong(line.substring(0, line.indexOf('\t')));
                offer(heap, new Ranked(score, line), topN);
            }
            List<Ranked> sorted = new ArrayList<>(heap);
            sorted.sort((a, b) -> b.compareTo(a));
            int rank = 1;
            for (Ranked r : sorted) {
                // rank, score, count, avg severity, severe ratio, street|city|state
                ctx.write(NullWritable.get(), new Text(rank++ + "\t" + r.line));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            return JobUtil.usage("top-streets", "   (-Dargus.topN=25 -Dargus.minCount=20)");
        }
        Configuration conf = getConf();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path intermediate = new Path(args[1] + "_stage1");
        JobUtil.prepareOutput(conf, out);
        JobUtil.prepareOutput(conf, intermediate);

        Job aggregate = Job.getInstance(conf, "ARGUS: street aggregates (1/2)");
        aggregate.setJarByClass(TopDangerousStreets.class);
        aggregate.setMapperClass(StreetMapper.class);
        aggregate.setCombinerClass(SeverityStats.MergeReducer.class);
        aggregate.setReducerClass(SeverityStats.MergeReducer.class);
        aggregate.setOutputKeyClass(Text.class);
        aggregate.setOutputValueClass(SeverityWritable.class);
        aggregate.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(aggregate, in);
        FileOutputFormat.setOutputPath(aggregate, intermediate);
        if (!aggregate.waitForCompletion(true)) {
            return 1;
        }

        Job rank = Job.getInstance(conf, "ARGUS: top dangerous streets (2/2)");
        rank.setJarByClass(TopDangerousStreets.class);
        rank.setInputFormatClass(SequenceFileInputFormat.class);
        rank.setMapperClass(TopNMapper.class);
        rank.setReducerClass(TopNReducer.class);
        rank.setNumReduceTasks(1);
        rank.setMapOutputKeyClass(NullWritable.class);
        rank.setMapOutputValueClass(Text.class);
        rank.setOutputKeyClass(NullWritable.class);
        rank.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(rank, intermediate);
        FileOutputFormat.setOutputPath(rank, out);
        boolean ok = rank.waitForCompletion(true);
        intermediate.getFileSystem(conf).delete(intermediate, true);
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TopDangerousStreets(), args));
    }
}
