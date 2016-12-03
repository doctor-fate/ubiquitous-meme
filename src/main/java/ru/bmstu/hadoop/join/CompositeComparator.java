package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeComparator extends WritableComparator {
    public CompositeComparator() {
        super(CompositeWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeWritable x = (CompositeWritable) a;
        CompositeWritable y = (CompositeWritable) b;
        return x.getAirport() - y.getAirport();
    }
}