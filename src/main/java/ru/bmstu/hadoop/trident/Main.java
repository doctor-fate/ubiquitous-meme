package ru.bmstu.hadoop.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Main {
    public static void main(String[] args) {
        FlightsSpout spout = new FlightsSpout(new Fields("line"));

        TridentTopology topology = new TridentTopology();
        topology.newStream("flights", spout)
                .each(new Fields("line"), new FlightParseFunction(), new Fields("day", "delay", "cancelled"))
                .filter(new Fields("day", "delay", "cancelled"), new FlightDelayFilter())
                .partitionBy(new Fields("day"))
                .partitionAggregate(new Fields("day"), new FlightDayAggregator(), new Fields("statistics"))
                .parallelismHint(6)
                .each(new Fields("statistics"), new StatisticsPrinterFunction(), new Fields());

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);
        conf.put(FlightsSpout.POLL_DIR, args[0]);
        conf.put(FlightsSpout.PROCESSED_DIR, args[1]);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("L8", conf, topology.build());
        Utils.sleep(6000L);
        cluster.killTopology("L8");
        cluster.shutdown();
    }
}
