package com.kafkastreams.bank;

import java.util.Properties;

import java.time.Instant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Integer i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("stephane"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("alice"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();

    }


    public static ProducerRecord<String, String> newRandomTransaction(String name) {
      // creates an empty json {}
      ObjectNode transaction = JsonNodeFactory.instance.objectNode();

      // { "amount" : 46 } (46 is a random number between 0 and 100 excluded)
      Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

      // Instant.now() is to get the current time using Java 8
      Instant now = Instant.now();

      // we write the data to the json document
      transaction.put("name", name);
      transaction.put("amount", amount);
      transaction.put("time", now.toString());
      return new ProducerRecord<>("bank-transactions", name, transaction.toString());
  }

}


