package com.kafka.admin.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Чтение сообщений из топика с указанными параметрами
    public List<Map<String, Object>> consumeMessages(String topicName, String groupId, int maxMessages,
                                                   long pollTimeoutMs, boolean fromBeginning) {
        logger.info("Consuming messages from topic: {}, groupId: {}, maxMessages: {}, timeout: {}ms, fromBeginning: {}",
                   topicName, groupId, maxMessages, pollTimeoutMs, fromBeginning);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (fromBeginning) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            logger.debug("Consumer configured to read from beginning");
        } else {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            logger.debug("Consumer configured to read from latest");
        }

        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            logger.debug("Consumer created, subscribing to topic: {}", topicName);
            consumer.subscribe(Collections.singletonList(topicName));

            logger.debug("Polling for messages with timeout: {}ms", pollTimeoutMs);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            logger.debug("Polled {} records", records.count());

            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> messageData = new HashMap<>();
                messageData.put("partition", record.partition());
                messageData.put("offset", record.offset());
                messageData.put("key", record.key());
                messageData.put("value", record.value());
                messageData.put("timestamp", record.timestamp());

                messages.add(messageData);
                logger.trace("Added message from partition {} offset {}", record.partition(), record.offset());

                if (messages.size() >= maxMessages) {
                    logger.debug("Reached maximum message limit: {}", maxMessages);
                    break;
                }
            }

            // Ручной коммит смещений
            if (!messages.isEmpty()) {
                logger.debug("Committing offsets for {} messages", messages.size());
            consumer.commitSync();
        }
        } catch (Exception e) {
            logger.error("Error consuming messages from topic {}: {}", topicName, e.getMessage(), e);
            throw e;
    }

        logger.info("Successfully consumed {} messages from topic: {}", messages.size(), topicName);
        return messages;
    }

    // Чтение сообщений из конкретной партиции и смещения
    public List<Map<String, Object>> consumeFromPartitionAndOffset(String topicName, int partition,
                                                                 long offset, int maxMessages) {
        logger.info("Consuming messages from topic: {}, partition: {}, offset: {}, maxMessages: {}",
                   topicName, partition, offset, maxMessages);

        String randomGroupId = UUID.randomUUID().toString();
        logger.debug("Using random groupId: {}", randomGroupId);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, randomGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            logger.debug("Assigning consumer to partition: {} of topic: {}", partition, topicName);
            consumer.assign(Collections.singleton(topicPartition));

            logger.debug("Seeking to offset: {} in partition: {}", offset, partition);
            consumer.seek(topicPartition, offset);

            logger.debug("Polling for messages with 5000ms timeout");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            logger.debug("Polled {} records from partition {}", records.count(), partition);

            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> messageData = new HashMap<>();
                messageData.put("partition", record.partition());
                messageData.put("offset", record.offset());
                messageData.put("key", record.key());
                messageData.put("value", record.value());
                messageData.put("timestamp", record.timestamp());

                messages.add(messageData);
                logger.trace("Added message from partition {} offset {}", record.partition(), record.offset());

                if (messages.size() >= maxMessages) {
                    logger.debug("Reached maximum message limit: {}", maxMessages);
                    break;
}
            }
        } catch (Exception e) {
            logger.error("Error consuming messages from topic {} partition {} offset {}: {}",
                        topicName, partition, offset, e.getMessage(), e);
            throw e;
        }

        logger.info("Successfully consumed {} messages from topic: {} partition: {} starting at offset: {}",
                   messages.size(), topicName, partition, offset);
        return messages;
    }
}
