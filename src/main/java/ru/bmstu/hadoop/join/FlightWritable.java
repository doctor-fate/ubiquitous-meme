package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class FlightWritable implements Writable {
    static Optional<FlightWritable> parseFlight(String s) {
        String[] split = s.split(",");
        try {
            FlightWritable w = new FlightWritable();
            if (!split[19].equals("1.00")) {
                w.delay = Float.parseFloat(split[18]);
            }
            return Optional.of(w);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return Optional.empty();
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