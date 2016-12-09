package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class FlightWritable implements Writable {
    @SuppressWarnings("unused")
    public FlightWritable() { }

    private FlightWritable(int code, float delay) {
        this.code = code;
        this.delay = delay;
    }

    static Optional<FlightWritable> parseLine(String s) {
        String[] split = s.split(",");
        FlightWritable w = null;
        try {
            int code = Integer.parseInt(split[14]);
            boolean cancelled = split[19].equals("1.00");
            float delay = 0.0f;
            if (!cancelled) {
                delay = Float.parseFloat(split[18]);
            }
            w = new FlightWritable(code, delay);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(w);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(code);
        out.writeFloat(delay);
    }

    public void readFields(DataInput in) throws IOException {
        code = in.readInt();
        delay = in.readFloat();
    }

    boolean isDelayed() {
        return Float.compare(delay, 0.0f) == 1;
    }

    float getDelay() {
        return delay;
    }

    int getCode() {
        return code;
    }

    private int code;
    private float delay;
}