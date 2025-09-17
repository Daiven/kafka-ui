package com.kafka.admin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;
@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        logger.info("Initializing KafkaProducerService");
        this.kafkaTemplate = kafkaTemplate;
        logger.debug("KafkaProducerService initialized successfully");
    }

    // Отправка сообщения в топик
    public void sendMessage(String topicName, String key, String message) {
        logger.info("Sending message to topic: {} with key: {}", topicName, key);
        logger.debug("Message content: {}", message);

        CompletableFuture<SendResult<String, String>> future;

        if (key != null && !key.isEmpty()) {
            logger.debug("Sending message with key");
            future = kafkaTemplate.send(topicName, key, message);
        } else {
            logger.debug("Sending message without key");
            future = kafkaTemplate.send(topicName, message);
        }

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Successfully sent message to topic [{}] with offset [{}] on partition [{}]",
                           topicName, result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
                logger.debug("Message content: [{}]", message);
            } else {
                logger.error("Failed to send message to topic [{}]: {}", topicName, ex.getMessage(), ex);
                logger.debug("Failed message content: [{}]", message);
            }
        });
    }
}