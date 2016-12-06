package ru.bmstu.hadoop.hbase.actors;

import com.google.common.base.Charsets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.bmstu.hadoop.hbase.filters.FlightDelayFilter;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class FlightsActor {
    static {
        TABLE_NAME = TableName.valueOf("FLIGHTS");
        FAMILY = new byte[]{ 'f', 'l', 'i', 'g', 'h', 't' };
    }

    public FlightsActor(Configuration configuration) {
        this.configuration = configuration;
    }

    public void createTable() throws IOException {
        try (Connection connection = createConnection()) {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(TABLE_NAME)) {
                deleteTable(admin);
            }

            HTableDescriptor descriptor = new HTableDescriptor(TABLE_NAME);
            HColumnDescriptor family = new HColumnDescriptor(FAMILY);
            descriptor.addFamily(family);
            admin.createTable(descriptor);
        }
    }

    private Connection createConnection() throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

    private void deleteTable(Admin admin) throws IOException {
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
    }

    public void setContentFromCSV(String filename) throws IOException {
        try (Connection connection = createConnection();
             Table table = connection.getTable(TABLE_NAME);
             CSVParser parser = CSVParser.parse(new File(filename), Charsets.UTF_8,
                     CSVFormat.DEFAULT.withHeader().withTrailingDelimiter())) {

            for (CSVRecord r : parser.getRecords()) {
                addToTable(r, table);
            }
        }
    }

    private static void addToTable(CSVRecord r, Table table) throws IOException {
        byte[] key = Bytes.toBytes(String.format("%s;%s;%05d", r.get(5), r.get(7), r.getRecordNumber()));
        Put put = new Put(key);
        for (Map.Entry<String, String> e : r.toMap().entrySet()) {
            put.addColumn(FAMILY, Bytes.toBytes(e.getKey().toLowerCase()), Bytes.toBytes(e.getValue()));
        }
        table.put(put);
    }

    public void scan(String from, String to, float delay) throws IOException {
        byte[] start = Bytes.toBytes(String.format("%s;00000;00000", from));
        byte[] end = Bytes.toBytes(String.format("%s;99999;99999", to));

        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        scan.setFilter(new FlightDelayFilter(delay, FAMILY));
        scan.setStartRow(start);
        scan.setStopRow(end);

        try (Connection connection = createConnection();
             Table table = connection.getTable(TABLE_NAME);
             ResultScanner scanner = table.getScanner(scan)) {

            for (Result r : scanner) {
                prettyPrint(r);
            }
        }
    }

    private static void prettyPrint(Result r) {
        System.out.printf("%s --> ", new String(r.getRow()));
        for (Cell v : r.rawCells()) {
            System.out.printf("%s;", new String(CellUtil.cloneValue(v)));
        }
        System.out.println();
    }

    private static final TableName TABLE_NAME;
    private static final byte[] FAMILY;
    private Configuration configuration;
}
