package ru.bmstu.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<FlightWritable, Text> {
    @Override
    public int getPartition(FlightWritable key, Text text, int i) {
        return Math.round(key.getDelay()) % i;
    }
}