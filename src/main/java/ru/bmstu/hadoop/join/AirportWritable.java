package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class AirportWritable implements Writable {
    @SuppressWarnings("unused")
    public AirportWritable() { }

    private AirportWritable(int code, String name) {
        this.code = code;
        this.name = name;
    }

    static Optional<AirportWritable> parseLine(String s) {
        String[] split = s.split(",", 2);
        AirportWritable w = null;
        try {
            int code = Integer.parseInt(split[0].replaceAll("\"", ""));
            String name = split[1].trim();
            w = new AirportWritable(code, name);
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