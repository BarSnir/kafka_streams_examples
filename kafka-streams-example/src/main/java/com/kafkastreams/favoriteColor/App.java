package com.kafkastreams.favoriteColor;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class App {

    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("color-input-stream-v1");
        KStream<String, String>favoriteColorTable = textLines
            // 1 - we ensure that a comma is here as we will split on it
            .filter((key, value) -> value.contains(","))
            // 2 - we select a key that will be the user id (lowercase for safety)
            .selectKey((key, value) -> value.split(",")[0].toLowerCase())
            // 3 - we get the colour from the value (lowercase for safety)
            .mapValues(value -> value.split(",")[1].toLowerCase())
            // 4 - we filter undesired colours (could be a data sanitization step
            .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        favoriteColorTable.to("color-table-v1");
        // Arrange table
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        KTable<String, String> colorsTable = builder.table("color-table-v1");

        // Grouping & Agg (count())
        KTable<String, Long> favoriteColorTableAgg = colorsTable
            // 5 - we group by color within the KTable
            .groupBy((user, color) -> new org.apache.kafka.streams.KeyValue<>(color, color))
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColors")
                .withKeySerde(stringSerde)
                .withValueSerde(longSerde)
            );
        favoriteColorTableAgg.toStream().to(
            "color-table-agg-results-v1", 
            Produced.with(Serdes.String(),Serdes.Long())
        );
        return builder.build();
    }
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

        App favoriteColor = new App();
        KafkaStreams streams = new KafkaStreams(favoriteColor.createTopology(), properties);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.metadataForLocalThreads().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
