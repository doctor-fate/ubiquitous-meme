package sort;

import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements WritableComparable<FlightWritable> {
    static FlightWritable parseFlight(String s) throws NumberFormatException {
        FlightWritable w = new FlightWritable();

        String[] split = s.split(",");
        w.destination = Integer.parseInt(split[14]);
        w.cancelled = split[19].equals("1.00");
        if (!w.cancelled) {
            w.time = Float.parseFloat(split[21]);
            w.delay = Float.parseFloat(split[18]);
        }

        return w;
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

    private int destination;
    private float time;
    private float delay;
    private boolean cancelled;
}