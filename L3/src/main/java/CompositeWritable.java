import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeWritable implements WritableComparable<CompositeWritable> {
    static CompositeWritable getInstance(int airport, int flag) {
        CompositeWritable cw = new CompositeWritable();
        cw.airport = airport;
        cw.flag = flag;

        return cw;
    }

    public int compareTo(@NotNull CompositeWritable o) {
        int r = airport - o.airport;
        if (r == 0) {
            return flag - o.flag;
        }

        return r;
    }

    @Override
    public int hashCode() {
        int result = airport;
        result = 31 * result + flag;
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

        CompositeWritable that = (CompositeWritable) o;
        return airport == that.airport && flag == that.flag;
    }

    @Override
    public String toString() {
        return String.format("CompositeWritable{airport=%d, flag=%d}", airport, flag);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airport);
        out.writeInt(flag);
    }

    public void readFields(DataInput in) throws IOException {
        airport = in.readInt();
        flag = in.readInt();
    }

    int getAirport() {
        return airport;
    }

    private int airport;
    private int flag;
}