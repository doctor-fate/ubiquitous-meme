import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class FlightAirportWritable extends GenericWritable {
    static {
        CLASSES = (Class<? extends Writable>[]) new Class[]{
                FlightWritable.class,
                AirportWritable.class,
        };
    }

    static FlightAirportWritable createInstance(Writable instance) {
        FlightAirportWritable gw = new FlightAirportWritable();
        gw.set(instance);
        return gw;
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

    private static Class<? extends Writable>[] CLASSES;
}
