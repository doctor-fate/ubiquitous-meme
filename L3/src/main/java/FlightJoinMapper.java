import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightJoinMapper extends Mapper<LongWritable, Text, CompositeWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            FlightWritable w = FlightWritable.parseFlight(value.toString());
            if (!w.isDelayed()) {
                return;
            }

            int airport = getAirport(value.toString());
            context.write(CompositeWritable.getInstance(airport, 1), FlightAirportWritable.createInstance(w));
        } catch (NumberFormatException e) {
            System.err.println(value);
        }
    }

    private int getAirport(String s) throws NumberFormatException {
        String[] split = s.split(",");
        return Integer.parseInt(split[14]);
    }
}