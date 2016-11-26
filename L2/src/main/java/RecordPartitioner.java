import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RecordPartitioner extends Partitioner<RecordWritable, Text> {
    @Override
    public int getPartition(RecordWritable key, Text text, int i) {
        return Math.round(key.getDelay()) % i;
    }
}