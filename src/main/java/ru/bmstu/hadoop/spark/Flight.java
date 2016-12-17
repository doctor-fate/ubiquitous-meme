package ru.bmstu.hadoop.spark;

import org.apache.commons.validator.routines.FloatValidator;
import scala.Serializable;

class Flight implements Serializable {

    public Flight(boolean cancelled, float delay) {
        this.cancelled = cancelled;
        this.delay = delay;
    }

    static Flight parseLine(String s) {
        String[] split = s.split(",");

        float delay = 0.0f;
        boolean cancelled = split[CANCELLED_CSV_IDX].equals("1.00");
        if (!cancelled) {
            FloatValidator v = FloatValidator.getInstance();
            if (v.isValid(split[DELAY_CSV_IDX])) {
                delay = v.validate(split[DELAY_CSV_IDX]);
            }
        }

        return new Flight(cancelled, delay);
    }

    boolean isCancelledOrDelayed() {
        return cancelled || Float.compare(delay, 0.0f) == 1;
    }

    float getDelay() {
        return delay;
    }

    private static final int CANCELLED_CSV_IDX = 19;
    private static final int DELAY_CSV_IDX = 18;

    private boolean cancelled;
    private float delay;
}
