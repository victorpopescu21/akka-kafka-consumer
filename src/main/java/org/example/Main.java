package org.example;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.example.entity.ConsumerEntity;

import java.util.logging.Logger;


public class Main {


    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();

        Logger logger = Logger.getLogger(Main.class.getName());
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
         * Using multiple flows using .via()
         * */

        Flow<ConsumerRecord<String, String>, String, NotUsed> valueExtract = Flow.fromFunction(ConsumerRecord::value);

        Flow<String, ConsumerEntity, NotUsed> deserialize = Flow.fromFunction(value ->
        {
            logger.info("Deserializing: " + value);
            try {
                return mapper.readValue(value, ConsumerEntity.class);
            } catch (Exception e) {
                logger.warning("Error deserializing: " + value);
                return new ConsumerEntity(-1, "error", "error");
            }
        });

        Flow<ConsumerEntity, String, NotUsed> extractEmail = Flow.fromFunction(ConsumerEntity::getEmail);


        /*
         * Consumer.plainSource - Creates a source that consumes messages from the Kafka topic "victor-topic"
         * .aysnc() - introduces async boundaries between the stages of the stream. This is useful when the stream is doing some blocking operation
         * Parallelism is not controlled by async boundaries. It only controls the execution context. The stages still run synchronously
         *
         * mapAsync - Similar to async, but it also controls the parallelism of the stream. This ensures async processing of the stream
         * */
        Consumer
                .plainSource(consumerSettings, Subscriptions.topics("victor-topic"))
                .via(valueExtract).async()
                .via(deserialize).async()
                .via(extractEmail).async()
                .to(Sink.foreach(System.out::println))
                .run(materializer);

    }
}