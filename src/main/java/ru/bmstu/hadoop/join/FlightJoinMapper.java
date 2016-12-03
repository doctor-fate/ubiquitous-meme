package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class FlightJoinMapper extends Mapper<LongWritable, Text, CompositeWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        Optional<FlightWritable> flight = FlightWritable.parseFlight(s);
        if (!flight.isPresent()) {
            return;
        }
        FlightWritable w = flight.get();
        if (w.isDelayed()) {
            Optional<Integer> airport = getAirport(s);
            if (airport.isPresent()) {
                context.write(new CompositeWritable(airport.get(), 1), new FlightAirportWritable(w));
            }
        }
    }

    private Optional<Integer> getAirport(String s) {
        String[] split = s.split(",");
        try {
            int n = Integer.parseInt(split[14]);
            return Optional.of(n);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }
}