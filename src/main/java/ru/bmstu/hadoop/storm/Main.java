package ru.bmstu.hadoop.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Main {
    public static void main(String[] args) {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);
        conf.put(SentencesSpout.POLL_DIR, args[0]);
        conf.put(SentencesSpout.PROCESSED_DIR, args[1]);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("generator", new SentencesSpout(), 1);
        builder.setBolt("splitter", new SentenceSplitterBolt(), 3).shuffleGrouping("generator", "sentences");
        builder.setBolt("counter", new WordCounterBolt(), 5)
               .fieldsGrouping("splitter", new Fields("word"))
               .allGrouping("generator", "sync");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("L7", conf, builder.createTopology());
        Utils.sleep(16000L);
        cluster.killTopology("L7");
        cluster.shutdown();
    }
}
