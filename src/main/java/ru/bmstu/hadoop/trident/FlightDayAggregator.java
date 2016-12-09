package ru.bmstu.hadoop.trident;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class FlightDayAggregator implements CombinerAggregator<Map<Integer, Integer>> {
    @Override
    public Map<Integer, Integer> init(TridentTuple tuple) {
        Map<Integer, Integer> m = new HashMap<>();
        m.put(tuple.getInteger(0), 1);
        return m;
    }

    @Override
    public Map<Integer, Integer> combine(Map<Integer, Integer> a, Map<Integer, Integer> b) {
        Map<Integer, Integer> m = new HashMap<>();
        m.putAll(a);
        for (Map.Entry<Integer, Integer> e : b.entrySet()) {
            Integer k = m.getOrDefault(e.getKey(), 0);
            m.put(e.getKey(), k + e.getValue());
        }

        return m;
    }

    @Override
    public Map<Integer, Integer> zero() {
        return new HashMap<>();
    }
}
