package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        TableName name = TableName.valueOf("FLIGHTS");
        byte[] family = Bytes.toBytes("flight");

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            deleteTableIfExists(admin, name);
            createTable(admin, name, family);
            fillTable(connection, name, family, args[0]);
            scanTable(connection, name, family);
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

    private static void createTable(Admin admin, TableName name, byte[] fn) throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(name);
        HColumnDescriptor family = new HColumnDescriptor(fn);
        descriptor.addFamily(family);
        admin.createTable(descriptor);
    }

    private static void fillTable(Connection connection, TableName name, byte[] family, String f) throws IOException {
        try (Table table = connection.getTable(name); BufferedReader in = new BufferedReader(new FileReader(f))) {
            String line = in.readLine();
            byte[][] qualifiers = parseQualifiers(line);
            int p = 1;
            for (line = in.readLine(); line != null; line = in.readLine(), p++) {
                addToTable(line, p, family, qualifiers, table);
            }
        }
    }

    private static void scanTable(Connection connection, TableName name, byte[] family) throws IOException {
        try (Table table = connection.getTable(name)) {
            Scan scan = new Scan();

            scan.setFilter(new FlightDelayFilter(27.0f, family));
            scan.setStartRow(Bytes.toBytes("2015-01-13;00000;00000"));
            scan.setStopRow(Bytes.toBytes("2015-01-16;99999;99999"));

            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                prettyPrint(r);
            }
            scanner.close();
        }
    }

    private static byte[][] parseQualifiers(String line) {
        String[] split = line.replaceAll("\"", "").split(",");
        byte[][] q = new byte[split.length][];
        for (int i = 0; i < split.length; i++) {
            q[i] = Bytes.toBytes(split[i].trim().toLowerCase());
        }

        return q;
    }

    private static void addToTable(String line, int p, byte[] family, byte[][] qualifiers, Table table) throws IOException {
        String[] split = line.replaceAll("\"", "").split(",");
        String[] values = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            values[i] = split[i].trim();
        }

        byte[] key = Bytes.toBytes(String.format("%s;%s;%05d", values[5], values[7], p));
        Put put = new Put(key);
        for (int i = 0; i < values.length; i++) {
            put.addColumn(family, qualifiers[i], Bytes.toBytes(values[i]));
        }
        table.put(put);
    }

    private static void prettyPrint(Result r) {
        System.out.printf("%s --> ", new String(r.getRow()));
        for (Cell v : r.rawCells()) {
            System.out.printf("%s;", new String(CellUtil.cloneValue(v)));
        }
        System.out.println();
    }
}
