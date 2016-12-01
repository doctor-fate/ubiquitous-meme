package join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
    static FlightWritable parseFlight(String s) throws NumberFormatException {
        FlightWritable w = new FlightWritable();

        String[] split = s.split(",");
        if (!split[19].equals("1.00")) {
            w.delay = Float.parseFloat(split[18]);
        }

        return w;
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