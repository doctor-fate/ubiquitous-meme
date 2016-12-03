package ru.bmstu.hadoop.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class SortMapper extends Mapper<LongWritable, Text, FlightWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<FlightWritable> opt = FlightWritable.parseFlight(value.toString());
        if (opt.isPresent()) {
            FlightWritable w = opt.get();
            if (w.isCancelledOrDelayed()) {
                context.write(w, value);
            }
        }
    }
}