import scala.Serializable;

class Flight implements Serializable {
    static Flight getInstance(String s) {
        String[] split = s.split(",");

        Flight k = new Flight();
        k.cancelled = split[19].equals("1.00");
        if (k.cancelled) {
            return k;
        }

        try {
            k.delay = Float.parseFloat(split[18]);
        } catch (NumberFormatException e) {
            System.err.println(s);
        }

        return k;
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
