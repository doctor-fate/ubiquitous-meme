package ru.bmstu.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import ru.bmstu.hadoop.hbase.actors.FlightsHTableExecutor;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        FlightsHTableExecutor actor = new FlightsHTableExecutor(configuration);
        actor.createTable();
        actor.setContentFromCSV(args[0]);

        float delay = Float.parseFloat(args[1]);
        actor.scan(args[2], args[3], delay);
    }
}
