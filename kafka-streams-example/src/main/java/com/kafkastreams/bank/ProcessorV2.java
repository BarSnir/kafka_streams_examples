package com.kafkastreams.bank;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

public class ProcessorV2 {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Long> bankTransactions = builder.stream("bank-transactions");

        KTable<String, Long> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(
                    (aggValue, newValue) -> aggValue + newValue 
                );
        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.metadataForLocalThreads().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}