package com.example;

import com.example.DTO.InteractionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ActionConsumer {
    private static final Logger log = LoggerFactory.getLogger(ActionConsumer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "action-service-group";
    private static final String TOPIC_NAME = "incoming-interactions";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        log.info("Action Service started. Listening for interactions...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Kafka'dan gelen JSON String'ini tekrar Java objesine (POJO) çeviriyoruz
                        InteractionEvent event = objectMapper.readValue(record.value(), InteractionEvent.class);

                        processEvent(event);

                    } catch (Exception e) {
                        log.error("Failed to parse JSON message: {}", record.value(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Consumer error", e);
        } finally {
            consumer.close();
        }
    }

    private static void processEvent(InteractionEvent event) {
        switch (event.getInteractionType()) {
            case "DIRECT_MESSAGE":
                log.info("[ACTION] Sending automated greeting to DM from: @{}", event.getUsername());
                break;
            case "COMMENT":
                log.info("[ACTION] Running profanity check on comment by: @{}", event.getUsername());
                break;
            case "LIKE":
                log.info("[ACTION] Updating like counter for user: @{}", event.getUsername());
                break;
            default:
                log.warn("Unknown interaction type received!");
        }
    }
}