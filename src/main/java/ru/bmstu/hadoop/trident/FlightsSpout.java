package ru.bmstu.hadoop.trident;

import com.google.common.io.Files;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlightsSpout implements IBatchSpout {
    final static String POLL_DIR = "FlightsSpout_POLL_DIR";
    final static String PROCESSED_DIR = "FlightsSpout_PROCESSED_DIR";

    FlightsSpout(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        processed = (String) conf.get(PROCESSED_DIR);
        sources = Files.fileTreeTraverser().children(new File((String) conf.get(POLL_DIR))).iterator();
        batches = new HashMap<>();
    }

    @Override
    public void emitBatch(long id, TridentCollector collector) {
        List<String> batch = batches.computeIfAbsent(id, k -> readNextFile());
        if (batch != null) {
            batch.forEach(s -> collector.emit(new Values(s)));
        }
    }

    @Override
    public void ack(long id) {
        batches.remove(id);
    }

    @Override
    public void close() { }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

    private List<String> readNextFile() {
        if (!sources.hasNext()) {
            return null;
        }

        File current = sources.next();
        if (!current.isFile() || !current.canRead()) {
            return readNextFile();
        }
        FileReader r;
        try {
            r = new FileReader(current);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return readNextFile();
        }

        List<String> lines;
        try (BufferedReader br = new BufferedReader(r)) {
            lines = br.lines().collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return readNextFile();
        }

        try {
            Files.move(current, new File(processed + '/' + current.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines;
    }

    private Fields fields;
    private HashMap<Long, List<String>> batches;
    private Iterator<File> sources;
    private String processed;
}
