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

public class AnalyticsConsumer {
    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final String GROUP_ID = "analytics-service-group";
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

        log.info("Analytics Service started. Monitoring data flow...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        InteractionEvent event = objectMapper.readValue(record.value(), InteractionEvent.class);
                        analyzeData(event);
                    } catch (Exception e) {
                        log.error("JSON parse error in analytics", e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Consumer error", e);
        } finally {
            consumer.close();
        }
    }

    private static void analyzeData(InteractionEvent event) {
        log.info("[STATISTICS] Metric updated for event type: {}", event.getInteractionType());

        if (event.getContent() != null && event.getContent().length() > 30) {
            log.warn("[ALERT] Long message detected from user @{}. Content length: {}",
                    event.getUsername(), event.getContent().length());
        }
    }
}