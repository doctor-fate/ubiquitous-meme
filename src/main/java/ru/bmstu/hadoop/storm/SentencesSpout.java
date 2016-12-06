package ru.bmstu.hadoop.storm;

import com.google.common.io.Files;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.*;

public class SentencesSpout extends BaseRichSpout {
    final static String POLL_DIR = "SentencesSpout_POLL_DIR";
    final static String PROCESSED_DIR = "SentencesSpout_PROCESSED_DIR";

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        processed = (String) conf.get(PROCESSED_DIR);
        sources = Files.fileTreeTraverser().children(new File((String) conf.get(POLL_DIR))).iterator();
        send = new HashSet<>();
        reader = nextFile();
    }

    @Override
    public void nextTuple() {
        if (!readyToRead()) {
            Utils.sleep(100L);
            return;
        }

        sendNextLine();
    }

    private boolean readyToRead() {
        return reader != null;
    }

    private void sendNextLine() {
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (line == null) {
            closeReader();
        } else {
            UUID id = UUID.randomUUID();
            collector.emit("sentences", new Values(line), id);
            send.add(id);
        }
    }

    private void closeReader() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        reader = null;
    }

    @Override
    public void ack(Object id) {
        send.remove(id);
        if (send.isEmpty() && !readyToRead()) {
            sync();
        }
    }

    private void sync() {
        collector.emit("sync", new Values());
        moveFile();
        reader = nextFile();
    }

    private void moveFile() {
        try {
            Files.move(current, new File(processed + '/' + current.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private BufferedReader nextFile() {
        if (!sources.hasNext()) {
            return null;
        }

        current = sources.next();
        if (!current.isFile() || !current.canRead()) {
            return nextFile();
        }
        FileReader r;
        try {
            r = new FileReader(current);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return nextFile();
        }
        return new BufferedReader(r);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("sentences", new Fields("sentence"));
        declarer.declareStream("sync", new Fields());
    }

    private Set<Object> send;
    private BufferedReader reader;
    private Iterator<File> sources;
    private File current;
    private String processed;
    private SpoutOutputCollector collector;
}
