package sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlightWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            FlightWritable w = FlightWritable.parseFlight(value.toString());
            if (w.isCancelledOrDelayed()) {
                context.write(w, value);
            }
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
        }
    }
}