package com.argus.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Accidents per (state, hour of day).
 * Shows: combiner, custom Partitioner (a state's 24 hours land in one reducer), counters.
 */
public class StateHourCount extends Configured implements Tool {

    public static class StateHourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();

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
            outKey.set(a.state + "\t" + String.format("%02d", a.hour));
            ctx.write(outKey, ONE);
        }
    }

    public static class StatePartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String k = key.toString();
            int tab = k.indexOf('\t');
            String state = tab < 0 ? k : k.substring(0, tab);
            return (state.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            return JobUtil.usage("state-hour", "");
        }
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        JobUtil.prepareOutput(getConf(), out);

        Job job = Job.getInstance(getConf(), "ARGUS: accidents per state x hour");
        job.setJarByClass(StateHourCount.class);
        job.setMapperClass(StateHourMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setPartitionerClass(StatePartitioner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (getConf().get("mapreduce.job.reduces") == null) {
            job.setNumReduceTasks(4);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StateHourCount(), args));
    }
}
