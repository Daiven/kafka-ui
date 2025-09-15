package com.kafka.admin.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaConsumerService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Чтение сообщений из топика с указанными параметрами
    public List<Map<String, Object>> consumeMessages(String topicName, String groupId, int maxMessages,
                                                   long pollTimeoutMs, boolean fromBeginning) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (fromBeginning) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        } else {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }

        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> messageData = new HashMap<>();
                messageData.put("partition", record.partition());
                messageData.put("offset", record.offset());
                messageData.put("key", record.key());
                messageData.put("value", record.value());
                messageData.put("timestamp", record.timestamp());

                messages.add(messageData);

                if (messages.size() >= maxMessages) {
                    break;
                }
            }

            // Ручной коммит смещений
            consumer.commitSync();
        }

        return messages;
    }

    // Чтение сообщений из конкретной партиции и смещения
    public List<Map<String, Object>> consumeFromPartitionAndOffset(String topicName, int partition,
                                                                 long offset, int maxMessages) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            consumer.assign(Collections.singleton(topicPartition));
            consumer.seek(topicPartition, offset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> messageData = new HashMap<>();
                messageData.put("partition", record.partition());
                messageData.put("offset", record.offset());
                messageData.put("key", record.key());
                messageData.put("value", record.value());
                messageData.put("timestamp", record.timestamp());

                messages.add(messageData);

                if (messages.size() >= maxMessages) {
                    break;
                }
            }
        }

        return messages;
    }
}