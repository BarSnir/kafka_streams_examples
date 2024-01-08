package com.kafkastreams.joins;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Stream2Global {

    public static void main(String[] args) {
        Properties props = getProperties();
        KafkaStreams stream = new KafkaStreams(getTopology(), props);
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
        while(true){
        try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "market-info-enrich-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7070");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }

    public static Topology getTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> ordersStream = getStreamJson("orders", builder);
        GlobalKTable<String, JsonNode> marketInfo = builder.globalTable("market-info-store");
        ordersStream.join(marketInfo,
            (key, value) -> key,
            (left,right) -> {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode jNode = mapper.createObjectNode();
                jNode = setFiled(jNode, "order", left);
                jNode = setFiled(jNode, "manufacturer_text", right);
                jNode = setFiled(jNode, "model_text", right);
                jNode = setFiled(jNode, "submodel_text", right);
                return (JsonNode) jNode;
            }

        );
        ordersStream.to(
            "orders_enriched",
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

    public static ObjectNode setFiled(ObjectNode jNode, String fieldName, JsonNode tableNode) {
        return ((ObjectNode) jNode).put(
            fieldName, tableNode.get(fieldName).asText()
        );
    }
}
