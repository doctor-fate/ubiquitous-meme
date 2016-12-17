package ru.bmstu.hadoop.join;

import org.apache.commons.validator.routines.FloatValidator;
import org.apache.commons.validator.routines.IntegerValidator;
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
        String[] split = s.replaceAll("\"", "").split(",");

        IntegerValidator iv = IntegerValidator.getInstance();
        if (!iv.isValid(split[CODE_CSV_IDX])) {
            return Optional.empty();
        }
        int code = iv.validate(split[CODE_CSV_IDX]);

        float delay = 0.0f;
        boolean cancelled = split[CANCELLED_CSV_IDX].equals("1.00");
        if (!cancelled) {
            FloatValidator fv = FloatValidator.getInstance();
            if (!fv.isValid(split[DELAY_CSV_IDX])) {
                return Optional.empty();
            }
            delay = fv.validate(split[DELAY_CSV_IDX]);
        }

        return Optional.of(new FlightWritable(code, delay));
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

    private static final int CODE_CSV_IDX = 14;
    private static final int CANCELLED_CSV_IDX = 19;
    private static final int DELAY_CSV_IDX = 18;

    private int code;
    private float delay;
}