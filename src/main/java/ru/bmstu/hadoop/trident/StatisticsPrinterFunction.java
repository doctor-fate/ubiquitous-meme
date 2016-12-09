package ru.bmstu.hadoop.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

public class StatisticsPrinterFunction extends BaseFunction {
    @SuppressWarnings("unchecked")
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<Integer, Integer> m = (Map<Integer, Integer>) tuple.get(0);
        m.entrySet().forEach(System.out::println);
    }
}
