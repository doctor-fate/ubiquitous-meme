import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
    static FlightWritable parseFlight(String s) throws NumberFormatException {
        String[] split = s.split(",");

        FlightWritable record = new FlightWritable();
        if (!split[19].equals("1.00")) {
            record.delay = Float.parseFloat(split[18]);
        }

        return record;
    }

    @Override
    public String toString() {
        return String.format("FlightWritable{delay=%f}", delay);
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(delay);
    }

    public void readFields(DataInput in) throws IOException {
        delay = in.readFloat();
    }

    boolean isDelayed() {
        return Float.compare(delay, 0.0f) == 1;
    }

    float getDelay() {
        return delay;
    }

    private float delay;
}