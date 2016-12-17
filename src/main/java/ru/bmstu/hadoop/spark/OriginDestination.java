package ru.bmstu.hadoop.spark;

import org.apache.commons.validator.routines.IntegerValidator;
import scala.Serializable;

public class OriginDestination implements Serializable {

    private OriginDestination(int origin, int destination) {
        this.origin = origin;
        this.destination = destination;
    }

    static OriginDestination parseLine(String s) {
        String[] split = s.split(",");

        int origin = 0, destination = 0;
        IntegerValidator v = IntegerValidator.getInstance();
        if (v.isValid(split[ORIGIN_CSV_IDX]) && v.isValid(split[DESTINATION_CSV_IDX])) {
            origin = v.validate(split[ORIGIN_CSV_IDX]);
            destination = v.validate(split[DESTINATION_CSV_IDX]);
        }

        return new OriginDestination(origin, destination);
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

    private static final int ORIGIN_CSV_IDX = 11;
    private static final int DESTINATION_CSV_IDX = 14;

    private int origin;
    private int destination;
}
