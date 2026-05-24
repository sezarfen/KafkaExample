package com.example;

import com.example.DTO.InteractionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class WebhookProducer {
    private static final Logger log = LoggerFactory.getLogger(WebhookProducer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "incoming-interactions";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Random random = new Random();
        String[] types = { "DIRECT_MESSAGE", "COMMENT", "LIKE" };
        String[] users = { "john_doe", "alice_smith", "tech_guru", "marketing_bot" };

        log.info("Starting Webhook Simulator...");

        try {
            while (true) {
                String eventId = UUID.randomUUID().toString();
                String type = types[random.nextInt(types.length)];
                String user = users[random.nextInt(users.length)];
                String content = type.equals("LIKE") ? "" : "Hello, I need help with my account!";

                InteractionEvent event = new InteractionEvent(eventId, type, user, content);

                String jsonValue = objectMapper.writeValueAsString(event);
                log.info("Sending event: " + jsonValue);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, eventId, jsonValue);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Sent: {} | Partition: {}", event.getInteractionType(), metadata.partition());
                    } else {
                        log.error("Error sending message", exception);
                    }
                });

                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.error("Producer interrupted", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}