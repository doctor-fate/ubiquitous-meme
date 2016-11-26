import org.apache.hadoop.mapreduce.Partitioner;

public class CompositePartitioner extends Partitioner<CompositeWritable, FlightAirportWritable> {
    @Override
    public int getPartition(CompositeWritable key, FlightAirportWritable w, int i) {
        return key.getAirport() % i;
    }
}