package com.kafka.admin.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Отправка сообщения в топик
    public void sendMessage(String topicName, String key, String message) {
        CompletableFuture<SendResult<String, String>> future;

        if (key != null && !key.isEmpty()) {
            future = kafkaTemplate.send(topicName, key, message);
        } else {
            future = kafkaTemplate.send(topicName, message);
        }

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                    "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
            }
        });
    }
}