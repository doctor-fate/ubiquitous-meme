package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyAirportComparator extends WritableComparator {
    public CompositeKeyAirportComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable x = (CompositeKeyWritable) a;
        CompositeKeyWritable y = (CompositeKeyWritable) b;
        return x.getAirport() - y.getAirport();
    }
}