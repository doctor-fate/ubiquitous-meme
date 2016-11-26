import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, RecordWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            RecordWritable record = RecordWritable.parseRecord(value.toString());
            if (record.isCancelledOrDelayed()) {
                context.write(record, value);
            }
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
        }
    }
}