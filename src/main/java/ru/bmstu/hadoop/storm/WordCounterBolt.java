package ru.bmstu.hadoop.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String stream = input.getSourceStreamId();
        if (stream.equals("sync")) {
            sync();
        } else {
            String word = input.getStringByField("word").toLowerCase();
            int v = dictionary.getOrDefault(word, 0);
            dictionary.put(word, v + 1);
        }
    }

    private void sync() {
        dictionary.entrySet().forEach(System.out::println);
        dictionary.clear();
    }

    private Map<String, Integer> dictionary = new HashMap<>();
}
