package ru.bmstu.hadoop.join;

import org.apache.commons.validator.routines.IntegerValidator;
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
        String[] split = s.replaceAll("\"", "").split(",", 2);

        IntegerValidator v = IntegerValidator.getInstance();
        if (!v.isValid(split[CODE_CSV_IDX])) {
            return Optional.empty();
        }
        int code = v.validate(split[CODE_CSV_IDX]);

        String name = split[NAME_CSV_IDX];

        return Optional.of(new AirportWritable(code, name));
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

    private static final int NAME_CSV_IDX = 1;
    private static final int CODE_CSV_IDX = 0;

    private int code;
    private String name;
}