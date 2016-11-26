import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportJoinMapper extends Mapper<LongWritable, Text, CompositeWritable, FlightAirportWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirportWritable w = AirportWritable.parseAirport(value.toString());
        try {
            int airport = getAirport(value.toString());
            context.write(CompositeWritable.getInstance(airport, 0), FlightAirportWritable.createInstance(w));
        } catch (NumberFormatException e) {
            System.err.println(value);
        }
    }

    private int getAirport(String s) throws NumberFormatException {
        String[] split = s.split(",", 2);
        String i = split[0].replace("\"", "").trim();
        return Integer.parseInt(i);
    }
}