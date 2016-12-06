package ru.bmstu.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class Main {
    private static Tuple2<Integer, String> parseAirport(String s) {
        String[] split = s.split(",", 2);
        String name = split[1].trim();
        int id = 0;
        try {
            id = Integer.parseInt(split[0].replace("\"", "").trim());
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return new Tuple2<>(id, name);
    }

    private static Tuple2<OriginDestination, Flight> parseFlight(String s) {
        OriginDestination e = OriginDestination.parseLine(s);
        Flight f = Flight.parseLine(s);
        return new Tuple2<>(e, f);
    }

    public static void main(String[] args) {
        SparkConf configuration = new SparkConf().setAppName("L5");
        JavaSparkContext context = new JavaSparkContext(configuration);

        JavaPairRDD<OriginDestination, Flight> flights;
        flights = context.textFile("resources/flights.csv")
                         .mapToPair(Main::parseFlight)
                         .filter((e) -> e._1.isValid());

        JavaPairRDD<OriginDestination, Statistics> stats;
        stats = flights.aggregateByKey(Statistics.ZERO, Statistics::add, Statistics::merge);

        JavaPairRDD<Integer, String> airports;
        airports = context.textFile("resources/airports.csv").mapToPair(Main::parseAirport).filter((e) -> e._1 != 0);

        final Broadcast<Map<Integer, String>> b = context.broadcast(airports.collectAsMap());
        stats.map((e) -> {
            OriginDestination k = e._1;
            Map<Integer, String> names = b.value();
            String origin = names.get(k.getOrigin());
            String destination = names.get(k.getDestination());

            return new Tuple2<>(new Tuple2<>(origin, destination), e._2);
        }).collect().forEach(System.out::println);
    }
}