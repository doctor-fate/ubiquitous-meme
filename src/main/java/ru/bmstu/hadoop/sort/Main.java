package ru.bmstu.hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main implements Tool {
    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(null, new Main(), args);
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration, "L2");
        job.setJarByClass(Main.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(FlightPartitioner.class);
        job.setGroupingComparatorClass(FlightComparator.class);

        job.setMapOutputKeyClass(FlightWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(8);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    private Configuration configuration;

    public Configuration getConf() {
        return configuration;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }
}
