package ru.bmstu.hadoop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class FlightDelayFilter extends FilterBase {
    FlightDelayFilter(float delay, byte[] family) {
        this.family = family;
        this.delay = delay;
        data = new FilterRoundData();
    }

    @SuppressWarnings("unused")
    public static Filter parseFrom(byte[] b) throws DeserializationException {
        float delay = Bytes.toFloat(b);
        byte[] family = Arrays.copyOfRange(b, 4, b.length);
        return new FlightDelayFilter(delay, family);
    }

    public void reset() {
        data.reset();
    }

    @Override
    public boolean filterRow() throws IOException {
        return data.mustFilter();
    }

    @Override
    public byte[] toByteArray() throws IOException {
        byte[] b = new byte[4 + family.length];
        Bytes.putFloat(b, 0, delay);
        Bytes.putBytes(b, 4, family, 0, family.length);
        return b;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (!data.mustFilter()) {
            return ReturnCode.INCLUDE;
        }

        if (data.mustSkip()) {
            return ReturnCode.NEXT_ROW;
        }

        if (CellUtil.matchingColumn(v, family, Bytes.toBytes("arr_delay_new"))) {
            String d = new String(CellUtil.cloneValue(v));
            float vd = 0.0f;
            try {
                vd = Float.parseFloat(d);
            } catch (NumberFormatException ignored) { }

            data.setDelayed(vd);
        }

        if (CellUtil.matchingColumn(v, family, Bytes.toBytes("cancelled"))) {
            String d = new String(CellUtil.cloneValue(v));
            data.setCancelled(d);
        }

        return ReturnCode.INCLUDE;
    }

    private class FilterRoundData {
        void reset() {
            foundDelayedColumn = false;
            delayed = false;
            foundCancelledColumn = false;
            cancelled = false;
        }

        boolean mustSkip() {
            return foundDelayedColumn && foundCancelledColumn && mustFilter();
        }

        boolean mustFilter() {
            return !cancelled && !delayed;
        }

        void setCancelled(String cancelled) {
            this.cancelled = cancelled.equals("1.00");
            foundCancelledColumn = true;
        }

        void setDelayed(float delay) {
            delayed = (Float.compare(delay, FlightDelayFilter.this.delay) == 1);
            foundDelayedColumn = true;
        }

        private boolean foundDelayedColumn;
        private boolean delayed;
        private boolean foundCancelledColumn;
        private boolean cancelled;
    }

    private float delay;
    private byte[] family;
    private FilterRoundData data;
}