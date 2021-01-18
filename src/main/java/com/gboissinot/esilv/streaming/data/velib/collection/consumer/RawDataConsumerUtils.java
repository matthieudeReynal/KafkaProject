package com.gboissinot.esilv.streaming.data.velib.collection.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.BOOTSTRAP_SERVERS;
import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.RAW_TOPIC_NAME;

/**
 * @author Gregory Boissinot
 */
public class RawDataConsumerUtils {

    public static void main(String[] args) {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-test-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using properties.
        final Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(RAW_TOPIC_NAME));

        final AtomicInteger counter = new AtomicInteger(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            System.out.println("Nb elements: " + counter.get());
        }));

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                counter.incrementAndGet();
                System.out.println(consumerRecord);
            }
        }
    }

}
