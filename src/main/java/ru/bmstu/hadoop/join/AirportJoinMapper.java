package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class AirportJoinMapper extends Mapper<LongWritable, Text, CompositeWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirportWritable w = AirportWritable.parseAirport(value.toString());
        Optional<Integer> opt = getAirport(value.toString());
        if (opt.isPresent()) {
            int airport = opt.get();
            context.write(new CompositeWritable(airport, 0), new FlightAirportWritable(w));
        }
    }

    private Optional<Integer> getAirport(String s) {
        String[] split = s.split(",", 2);
        String i = split[0].replace("\"", "").trim();
        try {
            int n = Integer.parseInt(i);
            return Optional.of(n);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }
}