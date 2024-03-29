package com.kafkastreams.app;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Hello world!
 *
 */
public class App 
{
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input-v3");
        KTable<String, Long> wordCounts = textLines
            .mapValues(textLine -> textLine.toLowerCase())
            .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
            .selectKey((key, word) -> word)
            .groupByKey()
            .count(Materialized.as("Counts"));

        wordCounts.toStream().to(
            "word-count-output-v3",
            Produced.with(Serdes.String(), Serdes.Long())
        );
        return builder.build();
    }

    public static void main( String[] args )
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        App wordCountApp = new App();
        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), properties);
        streams.start();
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            streams.metadataForLocalThreads().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
