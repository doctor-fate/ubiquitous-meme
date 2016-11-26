import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RecordComparator extends WritableComparator {
    public RecordComparator() {
        super(RecordWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RecordWritable x = (RecordWritable) a;
        RecordWritable y = (RecordWritable) b;
        return Float.compare(x.getDelay(), y.getDelay());
    }
}