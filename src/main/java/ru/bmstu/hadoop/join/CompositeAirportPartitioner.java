package ru.bmstu.hadoop.join;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeAirportPartitioner extends Partitioner<CompositeWritable, FlightAirportWritable> {
    @Override
    public int getPartition(CompositeWritable key, FlightAirportWritable w, int i) {
        return key.getAirport() % i;
    }
}