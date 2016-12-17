package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class FlightJoinMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<FlightWritable> opt = FlightWritable.parseLine(value.toString()).filter(FlightWritable::isDelayed);
        if (opt.isPresent()) {
            FlightWritable w = opt.get();
            context.write(new CompositeKeyWritable(w.getCode(), 1), new FlightAirportWritable(w));
        }
    }
}