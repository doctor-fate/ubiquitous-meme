package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class AirportWritable implements Writable {
    static Optional<AirportWritable> parseLine(String s) {
        String[] split = s.split(",", 2);
        AirportWritable w = null;
        try {
            w = new AirportWritable();
            w.code = Integer.parseInt(split[0].replaceAll("\"", ""));
            w.name = split[1].trim();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(w);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(code);
        out.writeUTF(name);
    }

    public void readFields(DataInput in) throws IOException {
        code = in.readInt();
        name = in.readUTF();
    }

    String getName() {
        return name;
    }

    int getCode() {
        return code;
    }

    private int code;
    private String name;
}