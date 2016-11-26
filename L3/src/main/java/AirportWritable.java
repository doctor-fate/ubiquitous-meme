import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    static AirportWritable parseAirport(String s) {
        String[] split = s.split(",", 2);
        String name = split[1].trim();
        AirportWritable record = new AirportWritable();
        record.name = name;

        return record;
    }

    @Override
    public String toString() {
        return String.format("AirportWritable{name=%s}", name);
    }

    public void write(DataOutput out) throws IOException {
        out.writeBytes(name);
    }

    public void readFields(DataInput in) throws IOException {
        name = in.readLine();
    }

    String getName() {
        return name;
    }

    private String name;
}