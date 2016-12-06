package ru.bmstu.hadoop.spark;

import scala.Serializable;

public class OriginDestination implements Serializable {
    static OriginDestination parseLine(String s) {
        String[] split = s.split(",");
        OriginDestination d = new OriginDestination();
        try {
            d.origin = Integer.parseInt(split[11]);
            d.destination = Integer.parseInt(split[14]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return d;
    }

    @Override
    public int hashCode() {
        int result = origin;
        result = 31 * result + destination;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OriginDestination that = (OriginDestination) o;
        return origin == that.origin && destination == that.destination;
    }

    boolean isValid() {
        return origin != 0 && destination != 0;
    }

    int getOrigin() {
        return origin;
    }

    int getDestination() {
        return destination;
    }

    private int origin;
    private int destination;
}
