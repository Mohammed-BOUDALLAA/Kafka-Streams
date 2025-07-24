import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class WeatherStreamApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-stream-app-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> weatherStream = builder.stream("weather-data");
        weatherStream.peek((k, v) -> System.out.println("🔹 Received: " + v));
        KStream<String, String> filtered = weatherStream.filter((key, value) -> {
            try {
                String[] parts = value.split(",");
                double temp = Double.parseDouble(parts[1]);
                return temp > 30;
            } catch (Exception e) {
                return false;
            }
        });
        filtered.peek((k, v) -> System.out.println("Filtered (>30°C): " + v));

        KStream<String, String> converted = filtered.mapValues(value -> {
            String[] parts = value.split(",");
            double celsius = Double.parseDouble(parts[1]);
            double fahrenheit = (celsius * 9 / 5) + 32;
            return parts[0] + "," + fahrenheit + "," + parts[2];
        });
        converted.peek((k, v) -> System.out.println("Converted to °F: " + v));
        KGroupedStream<String, String> grouped = converted
                .map((key, value) -> {
                    String[] parts = value.split(",");
                    return KeyValue.pair(parts[0], value);
                })
                .groupByKey();

        KTable<String, String> averages = grouped.aggregate(
                () -> "0.0,0.0,0",
                (key, value, aggregate) -> {
                    String[] valParts = value.split(",");
                    String[] aggParts = aggregate.split(",");

                    double sumTemp = Double.parseDouble(aggParts[0]) + Double.parseDouble(valParts[1]);
                    double sumHumidity = Double.parseDouble(aggParts[1]) + Double.parseDouble(valParts[2]);
                    int count = Integer.parseInt(aggParts[2]) + 1;

                    return sumTemp + "," + sumHumidity + "," + count;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> result = averages.toStream().mapValues(value -> {
            String[] parts = value.split(",");
            double avgTemp = Double.parseDouble(parts[0]) / Integer.parseInt(parts[2]);
            double avgHumidity = Double.parseDouble(parts[1]) / Integer.parseInt(parts[2]);
            return "Température Moyenne = " + avgTemp + "°F, Humidité Moyenne = " + avgHumidity + "%";
        });

        result.peek((k, v) -> System.out.println("Final Result: " + v));
        result.to("station-averages");
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
