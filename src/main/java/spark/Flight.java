package spark;

import scala.Serializable;

class Flight implements Serializable {
    Flight(String s) {
        String[] split = s.split(",");
        cancelled = split[19].equals("1.00");
        if (cancelled) {
            return;
        }

        try {
            delay = Float.parseFloat(split[18]);
        } catch (NumberFormatException e) {
            System.err.println(s);
        }
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
