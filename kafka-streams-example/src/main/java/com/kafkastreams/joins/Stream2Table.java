package com.kafkastreams.joins;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Stream2Table {
    public static void main(String[] args) {
        Properties props = getProperties();
        KafkaStreams stream = new KafkaStreams(getTopology(), props);
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
        while(true){
            stream.metadataForLocalThreads().forEach(data -> System.out.println(data));
            try {
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
        KStream<String,JsonNode> customers =  getStreamJson("customers", builder);
        KTable<String,JsonNode> addresses = getTableJson("addresses", builder);
        customers = customers.selectKey(
            (key, value)-> value.get("address_id").asText()
        );
        KStream<String,JsonNode> enrichedStream = customers.leftJoin(
            addresses,
            (left,right) -> {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode jNode = mapper.createObjectNode();
                jNode = setFiled(jNode, "address_name", right);
                jNode = setFiled(jNode, "address_id", right);
                jNode = setFiled(jNode, "customer_name", left);
                jNode = setFiled(jNode, "customer_id", left);
                return (JsonNode) jNode;
            }
        );
        enrichedStream.to(
            "address_enriched",
            Produced.with(
                Serdes.String(),
                getJsonSerde()
            )
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

    public static KStream<String,JsonNode> getStreamJson(String topic, StreamsBuilder builder) {
        Serde<JsonNode> jsonNodeSerde = getJsonSerde();
        return builder.stream(
            topic,
            Consumed.with(Serdes.String(), jsonNodeSerde)
        );
    }

    public static KTable<String,JsonNode> getTableJson(String topic, StreamsBuilder builder) {
        Serde<JsonNode> jsonNodeSerde = getJsonSerde();
        return builder.table(
            topic,
            Consumed.with(Serdes.String(), jsonNodeSerde)
        );
    }

    public static ObjectNode setFiled(ObjectNode jNode, String fieldName, JsonNode tableNode) {
        return ((ObjectNode) jNode).put(
            fieldName, tableNode.get(fieldName).asText()
        );
    }
}
