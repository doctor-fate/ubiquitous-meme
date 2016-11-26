import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements WritableComparable<RecordWritable> {
    static RecordWritable parseRecord(String s) throws NumberFormatException {
        String[] split = s.split(",");

        RecordWritable record = new RecordWritable();
        record.destination = Integer.parseInt(split[14]);
        record.cancelled = split[19].equals("1.00");
        if (!record.cancelled) {
            record.time = Float.parseFloat(split[21]);
            record.delay = Float.parseFloat(split[18]);
        }

        return record;
    }

    public int compareTo(@NotNull RecordWritable o) {
        if (cancelled && !o.cancelled) {
            return -1;
        } else if (!cancelled && o.cancelled) {
            return 1;
        }
        if (Float.compare(delay, o.delay) != 0) {
            return Float.compare(delay, o.delay);
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

        RecordWritable that = (RecordWritable) o;
        return destination == that.destination && Float.compare(that.time, time) == 0 &&
                Float.compare(that.delay, delay) == 0 && cancelled == that.cancelled;
    }

    @Override
    public String toString() {
        return String.format(
                "RecordWritable{destination=%d, time=%f, delay=%f, cancelled=%s}",
                destination, time, delay, cancelled
        );
    }

    public void write(DataOutput out) throws IOException {
        String s = destination + ";" + time + ";" + delay + ";" + cancelled;
        out.writeBytes(s);
    }

    public void readFields(DataInput in) throws IOException {
        String[] split = in.readLine().split(";");
        destination = Integer.parseInt(split[0]);
        time = Float.parseFloat(split[1]);
        delay = Float.parseFloat(split[2]);
        cancelled = Boolean.parseBoolean(split[3]);
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