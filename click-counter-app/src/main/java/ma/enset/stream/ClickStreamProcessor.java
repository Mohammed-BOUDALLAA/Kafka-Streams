package ma.enset.stream;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ClickStreamProcessor {

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> clickStream = builder.stream("clicks");

        // Optional: log every input
        clickStream.peek((k, v) -> System.out.println("Received click from userId: " + k));

        KTable<String, Long> clickCounts = clickStream
                .groupBy((key, value) -> key, Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        clickCounts
                .toStream()
                .peek((k, v) -> System.out.println("User: " + k + " -> Clicks: " + v))
                .to("click-counts", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
