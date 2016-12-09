package ru.bmstu.hadoop.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Optional;

public class FlightParseFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String line = tuple.getString(0);
        Flight.parseLine(line).ifPresent(
                w -> collector.emit(new Values(w.getDay(), w.getDelay(), w.isCancelled()))
        );
    }

    private static class Flight {
        private Flight(int day, float delay, boolean cancelled) {
            this.day = day;
            this.delay = delay;
            this.cancelled = cancelled;
        }

        static Optional<Flight> parseLine(String s) {
            String[] split = s.replaceAll("\"", "").split(",");
            Flight w = null;
            try {
                int day = Integer.parseInt(split[4]);
                boolean cancelled = split[19].equals("1.00");
                float delay = 0.0f;
                if (!cancelled) {
                    delay = Float.parseFloat(split[18]);
                }
                w = new Flight(day, delay, cancelled);
            } catch (NumberFormatException e) {
                /*e.printStackTrace();*/
            }

            return Optional.ofNullable(w);
        }

        int getDay() {
            return day;
        }

        float getDelay() {
            return delay;
        }

        boolean isCancelled() {
            return cancelled;
        }

        private int day;
        private float delay;
        private boolean cancelled;
    }
}
