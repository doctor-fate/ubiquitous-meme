package filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;

public class FlightDelayFilter extends FilterBase {
    public FlightDelayFilter(float delay, String family) {
        this.family = family.getBytes();
        this.delay = delay;
    }

    public static Filter parseFrom(byte[] b) throws DeserializationException {
        try {
            String[] split = new String(b).split(";");
            float delay = Float.parseFloat(split[0]);
            return new FlightDelayFilter(delay, split[1]);
        } catch (NumberFormatException e) {
            throw new DeserializationException(e);
        }
    }

    public void reset() {
        delayed = false;
        cancelled = false;
    }

    @Override
    public boolean filterRow() throws IOException {
        return !delayed && !cancelled;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        String b = String.format("%.2f;%s", delay, new String(family));
        return b.getBytes();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (delayed || cancelled) {
            return ReturnCode.INCLUDE;
        }

        if (CellUtil.matchingColumn(v, family, "arr_delay_new".getBytes())) {
            String d = new String(v.getValueArray(), v.getValueOffset(), v.getValueLength());
            float vd = 0.0f;
            try {
                vd = Float.parseFloat(d);
            } catch (NumberFormatException ignored) { }

            if (Float.compare(vd, delay) == 1) {
                delayed = true;
            }
        }

        if (CellUtil.matchingColumn(v, family, "cancelled".getBytes())) {
            String d = new String(v.getValueArray(), v.getValueOffset(), v.getValueLength());
            cancelled = d.equals("1.00");
        }

        return ReturnCode.INCLUDE;
    }

    private float delay;
    private byte[] family;
    private boolean delayed;
    private boolean cancelled;
}