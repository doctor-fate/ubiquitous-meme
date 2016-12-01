package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<CompositeWritable, FlightAirportWritable, Text, Text> {
    @Override
    protected void reduce(CompositeWritable key, Iterable<FlightAirportWritable> values, Context context) throws IOException, InterruptedException {
        float min = Float.MAX_VALUE, max = Float.MIN_VALUE, avg = 0;
        int n = 0;

        String name = null;
        for (FlightAirportWritable v : values) {
            Writable w = v.get();
            if (w instanceof AirportWritable) {
                name = ((AirportWritable) w).getName();
                continue;
            }
            float delay = ((FlightWritable) w).getDelay();
            min = Math.min(delay, min);
            max = Math.max(delay, max);
            avg += delay;
            n++;
        }

        if (n > 0) {
            context.write(new Text(name), new Text(String.format("(%.2f;%.2f;%.2f)", min, max, avg / n)));
        }
    }
}