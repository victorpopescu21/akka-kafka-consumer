package org.example;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.entity.ConsumerEntity;

public class Main {



    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        /*
        * ActorSystem - creates Akka Actor System named KafkaConsumerSystem, which is required for Akka Streams
        * Materializer - Converts Akka Streams blueprint into a running stream
        * */
        ActorSystem system = ActorSystem.create("KafkaConsumerSystem");
        Materializer materializer = Materializer.createMaterializer(system);

        /*
        * Creating Kafka consumer settings using Akka's ConsumerSettings class
        * new StringDeserializer() - Deserializes the key and value of the Kafka message as a string
        * withBootstrapServers("localhost:9092") - Specifies the Kafka broker to connect to
        * withGroupId("akka-consumer-group") - Specifies the consumer group ID
        * withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") - Specifies the offset to start consuming from - earliest means from the beginning of the topic
        * */
        ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("akka-consumer-group")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        /*
        * Consumer.plainSource - Creates a source that consumes messages from the Kafka topic "victor-topic"
        * */
        Consumer
                .plainSource(consumerSettings, Subscriptions.topics("victor-topic"))
                .map(ConsumerRecord::value)  // Get the message as a string
                .map(json -> {
                    System.out.println("🔍 Deserializing JSON: " + json);
                    try {
                        return mapper.readValue(json, ConsumerEntity.class);
                    } catch (Exception e) {
                        System.err.println("⚠️ Failed to deserialize JSON: " + json);
                        return new ConsumerEntity(-1, "N/A", "N/A");
                    }
                })
                .filter(entity -> entity != null) // Filter out failed deserialization attempts
                .map(ConsumerEntity::getEmail) // Get the email from the entity
                .runWith(Sink.foreach(System.out::println), materializer);
    }
}