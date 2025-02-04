package org.example;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.entity.ConsumerEntity;

import java.util.concurrent.CompletableFuture;

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
//        ConsumerSettings<String, String> consumerSettings =
//                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
//                        .withBootstrapServers("localhost:9092")
//                        .withGroupId("akka-consumer-group")
//                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//                        ;


        /*
         * Consumer.plainSource - Creates a source that consumes messages from the Kafka topic "victor-topic"
         * */
//
//        for (int i = 0; i < 3; i++) {
//            int consumerId = i;
//            Consumer.plainSource(consumerSettings, Subscriptions.topics("victor-topic"))
//                    .map(record -> {
//                        System.out.println("🔹 Consumer " + consumerId + " received: " + record.value());
//                        try {
//                            return mapper.readValue(record.value(), ConsumerEntity.class);
//                        } catch (Exception e) {
//                            System.err.println("⚠️ Consumer " + consumerId + " failed to deserialize: " + record.value());
//                            return new ConsumerEntity(-1, "N/A", "N/A");
//                        }
//                    })
//                    .runWith(Sink.foreach(entity -> System.out.println("✅ Consumer " + consumerId + " processed: " + entity)), materializer);
//        }
        String uniqueGroupId = "akka-consumer-group-" + System.currentTimeMillis();
        ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId(uniqueGroupId)
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // ✅ Manual offset commit

        // ✅ Start 3 independent consumers
        for (int i = 0; i < 3; i++) {
            int consumerId = i;
            new Thread(() -> {
                Consumer.committableSource(consumerSettings, Subscriptions.assignment(new TopicPartition("victor-topic", consumerId)))
                        .mapAsync(1, msg -> { // ✅ Ensures one message at a time
                            System.out.println("🔹 Consumer " + consumerId + " received: " + msg.record().value());
                            try {
                                ConsumerEntity entity = mapper.readValue(msg.record().value(), ConsumerEntity.class);
                                System.out.println("✅ Consumer " + consumerId + " processed: " + entity);

                                return msg.committableOffset().commitJavadsl(); // ✅ Commit offset after processing
                            } catch (Exception e) {
                                System.err.println("⚠️ Consumer " + consumerId + " failed to deserialize: " + msg.record().value());
                                return CompletableFuture.completedFuture(Done.done()); // ✅ Skip message, continue
                            }
                        })
                        .runWith(Sink.ignore(), materializer);
            }).start();
        }
    }
}