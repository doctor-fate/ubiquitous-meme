package ru.bmstu.hadoop.spark;

import scala.Serializable;

public class Statistics implements Serializable {
    final static Statistics ZERO;

    static {
        ZERO = new Statistics();
    }

    private Statistics() { }

    private Statistics(Statistics s, Flight f) {
        all = s.all + 1;
        if (f.isCancelledOrDelayed()) {
            delayed = s.delayed + 1;
            max = Math.max(s.max, f.getDelay());
        }
    }

    private Statistics(Statistics a, Statistics b) {
        delayed = a.delayed + b.delayed;
        all = a.all + b.all;
        max = Math.max(a.max, b.max);
    }

    Statistics add(Flight f) {
        return new Statistics(this, f);
    }

    Statistics merge(Statistics s) {
        return new Statistics(this, s);
    }

    @Override
    public String toString() {
        return String.format("(%.2f%%,%.2f)", ((float) delayed / all) * 100, max);
    }

    private int delayed;
    private int all;
    private float max;
}
