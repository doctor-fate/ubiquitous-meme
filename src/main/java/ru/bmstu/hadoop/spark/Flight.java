package ru.bmstu.hadoop.spark;

import scala.Serializable;

class Flight implements Serializable {
    static Flight parseFlight(String s) {
        String[] split = s.split(",");
        Flight f = new Flight();
        f.cancelled = split[19].equals("1.00");
        if (!f.cancelled) {
            try {
                f.delay = Float.parseFloat(split[18]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        return f;
    }

    boolean isCancelledOrDelayed() {
        return cancelled || Float.compare(delay, 0.0f) == 1;
    }

    float getDelay() {
        return delay;
    }

    private boolean cancelled;
    private float delay;
}
