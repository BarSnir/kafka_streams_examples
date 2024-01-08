package com.kafkastreams.joins;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.fasterxml.jackson.databind.JsonNode;

public class GlobalTable {

        public static Integer counter = 0;
        public static void main(String[] args) {
        Properties props = getProperties();
        KafkaStreams stream = new KafkaStreams(getTopology(), props);
        stream.start();
        ReadOnlyKeyValueStore<String, JsonNode> keyValueStore = stream.store(
            StoreQueryParameters.fromNameAndType(
                "market-info-store", QueryableStoreTypes.keyValueStore()
            )
        );
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
        while(true){
        try {
                KeyValueIterator<String, JsonNode> range = keyValueStore.all();
                printState(range);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "address-enrich-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }

    public static Topology getTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable(
            "market-info",
            Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("market-info-store")
            .withKeySerde(Serdes.String())
            .withValueSerde(getJsonSerde())
        );
        return builder.build();
    }

    public static Serde<JsonNode> getJsonSerde() {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(
            jsonSerializer,
            jsonDeserializer
        );
        return jsonSerde;
    }

    public static void printState(KeyValueIterator<String, JsonNode> range) {
        int internalCount = 0;
        while (range.hasNext()) {
            KeyValue<String, JsonNode> next = range.next();
            internalCount ++;
            if (internalCount > counter) {
                System.out.println(next.key + ": " + next.value);
                System.out.println(counter+"_AND_"+internalCount);
                counter++;
            }
        }
    }
}