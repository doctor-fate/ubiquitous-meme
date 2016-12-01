package join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    static AirportWritable parseAirport(String s) {
        AirportWritable w = new AirportWritable();

        String[] split = s.split(",", 2);
        w.name = split[1].trim();

        return w;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
    }

    String getName() {
        return name;
    }

    private String name;
}