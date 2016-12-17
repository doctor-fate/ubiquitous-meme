package ru.bmstu.hadoop.join;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeKeyAirportPartitioner extends Partitioner<CompositeKeyWritable, FlightAirportWritable> {
    @Override
    public int getPartition(CompositeKeyWritable key, FlightAirportWritable w, int i) {
        return key.getAirport() % i;
    }
}