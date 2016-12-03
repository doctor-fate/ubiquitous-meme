package ru.bmstu.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightComparator extends WritableComparator {
    public FlightComparator() {
        super(FlightWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlightWritable x = (FlightWritable) a;
        FlightWritable y = (FlightWritable) b;
        return Float.compare(x.getDelay(), y.getDelay());
    }
}