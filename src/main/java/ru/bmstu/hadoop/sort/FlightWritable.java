package ru.bmstu.hadoop.sort;

import org.apache.commons.validator.routines.FloatValidator;
import org.apache.commons.validator.routines.IntegerValidator;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class FlightWritable implements WritableComparable<FlightWritable> {

    @SuppressWarnings("unused")
    public FlightWritable() { }

    private FlightWritable(int destination, boolean cancelled, float time, float delay) {
        this.destination = destination;
        this.cancelled = cancelled;
        this.time = time;
        this.delay = delay;
    }

    static Optional<FlightWritable> parseLine(String s) {
        String[] split = s.replaceAll("\"", "").split(",");

        IntegerValidator iv = IntegerValidator.getInstance();
        if (!iv.isValid(split[DESTINATION_CSV_IDX])) {
            return Optional.empty();
        }
        int destination = iv.validate(split[DESTINATION_CSV_IDX]);

        float time = 0.0f, delay = 0.0f;
        boolean cancelled = split[CANCELLED_CSV_IDX].equals("1.00");
        if (!cancelled) {
            FloatValidator fv = FloatValidator.getInstance();
            if (!fv.isValid(split[TIME_CSV_IDX]) || !fv.isValid(split[DELAY_CSV_IDX])) {
                return Optional.empty();
            }
            time = fv.validate(split[TIME_CSV_IDX]);
            delay = fv.validate(split[DELAY_CSV_IDX]);
        }

        return Optional.of(new FlightWritable(destination, cancelled, time, delay));
    }

    public int compareTo(@NotNull FlightWritable o) {
        if (cancelled && !o.cancelled) {
            return -1;
        } else if (!cancelled && o.cancelled) {
            return 1;
        }

        int r = Float.compare(delay, o.delay);
        if (r != 0) {
            return r;
        }
        if (destination != o.destination) {
            return destination - o.destination;
        }

        return Float.compare(time, o.time);
    }

    @Override
    public int hashCode() {
        int result = destination;
        result = 31 * result + (time != +0.0f ? Float.floatToIntBits(time) : 0);
        result = 31 * result + (delay != +0.0f ? Float.floatToIntBits(delay) : 0);
        result = 31 * result + (cancelled ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FlightWritable that = (FlightWritable) o;
        return destination == that.destination && Float.compare(that.time, time) == 0 &&
                Float.compare(that.delay, delay) == 0 && cancelled == that.cancelled;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(destination);
        out.writeFloat(time);
        out.writeFloat(delay);
        out.writeBoolean(cancelled);
    }

    public void readFields(DataInput in) throws IOException {
        destination = in.readInt();
        time = in.readFloat();
        delay = in.readFloat();
        cancelled = in.readBoolean();
    }

    boolean isCancelledOrDelayed() {
        return cancelled || (Float.compare(delay, 0.0f) == 1);
    }

    float getDelay() {
        return delay;
    }

    private static final int DESTINATION_CSV_IDX = 14;
    private static final int TIME_CSV_IDX = 21;
    private static final int DELAY_CSV_IDX = 18;
    private static final int CANCELLED_CSV_IDX = 19;

    private int destination;
    private float time;
    private float delay;
    private boolean cancelled;
}