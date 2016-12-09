package ru.bmstu.hadoop.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class FlightDelayFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        Float delay = tuple.getFloat(1);
        Boolean cancelled = tuple.getBoolean(2);
        return cancelled || Float.compare(delay, 0.0f) == 1;
    }
}
