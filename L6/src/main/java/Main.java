import filters.FlightDelayFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("args.length < 1");
            System.exit(1);
        }

        final TableName tn = TableName.valueOf("FLIGHTS");
        final String fn = "flight";

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();

            deleteTableIfExists(admin, tn);
            createTable(admin, tn, fn);
            fillTable(connection, tn, fn, args[0]);

            scanTable(connection, tn, fn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void deleteTableIfExists(Admin admin, TableName name) throws IOException {
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }

    private static void createTable(Admin admin, TableName name, String fn) throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(name);
        HColumnDescriptor family = new HColumnDescriptor(fn);
        descriptor.addFamily(family);
        admin.createTable(descriptor);
    }

    private static void fillTable(Connection connection, TableName name, String family, String f) throws IOException {
        try (Table table = connection.getTable(name); BufferedReader in = new BufferedReader(new FileReader(f))) {
            String line = in.readLine();
            byte[][] qualifiers = parseQualifiers(line);
            int p = 1;
            for (line = in.readLine(); line != null; line = in.readLine(), p++) {
                addToTable(line, p, family.getBytes(), qualifiers, table);
            }
        }
    }

    private static void scanTable(Connection connection, TableName name, String family) throws IOException {
        try (Table table = connection.getTable(name)) {
            Scan scan = new Scan();

            scan.setFilter(new FlightDelayFilter(20.0f, family));
            scan.setStartRow("2015-01-24;00000;00000".getBytes());
            scan.setStopRow("2015-01-31;99999;99999".getBytes());

            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                prettyPrint(r);
            }
            scanner.close();
        }
    }

    private static byte[][] parseQualifiers(String line) {
        String[] split = line.split(",");
        byte[][] q = new byte[split.length][];
        for (int i = 0; i < split.length; i++) {
            q[i] = split[i].replaceAll("\"", "").trim().toLowerCase().getBytes();
        }

        return q;
    }

    private static void addToTable(String line, int p, byte[] family, byte[][] qualifiers, Table table) throws IOException {
        String[] split = line.split(",");
        String[] values = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            values[i] = split[i].replaceAll("\"", "").trim();
        }

        byte[] key = String.format("%s;%s;%05d", values[5], values[7], p).getBytes();
        Put put = new Put(key);
        for (int i = 0; i < values.length; i++) {
            put.addColumn(family, qualifiers[i], values[i].getBytes());
        }
        table.put(put);
    }

    private static void prettyPrint(Result r) {
        System.out.print(new String(r.getRow()) + " --> ");
        for (Cell v : r.rawCells()) {
            System.out.print(new String(v.getValueArray(), v.getValueOffset(), v.getValueLength()) + ";");
        }
        System.out.println();
    }
}
