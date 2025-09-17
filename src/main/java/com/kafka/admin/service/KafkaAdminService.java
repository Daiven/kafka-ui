package com.kafka.admin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaAdminService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        logger.info("Initializing KafkaAdminService");
        this.adminClient = adminClient;
        logger.debug("KafkaAdminService initialized successfully");
    }

    // Получение списка топиков
    public List<String> listTopics() throws ExecutionException, InterruptedException {
        logger.info("Listing all Kafka topics");
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();
            logger.info("Found {} topics", topicNames.size());
            logger.debug("Topic names: {}", topicNames);
            return new ArrayList<>(topicNames);
        } catch (Exception e) {
            logger.error("Error listing topics: {}", e.getMessage(), e);
            throw e;
        }
    }

    // Создание нового топика
    public void createTopic(String topicName, int numPartitions, short replicationFactor,
                           Map<String, String> configs) {
        logger.info("Creating topic: {} with {} partitions and replication factor {}",
                   topicName, numPartitions, replicationFactor);
        logger.debug("Topic configs: {}", configs);

        try {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            if (configs != null && !configs.isEmpty()) {
                logger.debug("Applying {} configurations to topic {}", configs.size(), topicName);
                newTopic.configs(configs);
            }

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            // Ждем завершения операции
            result.all().get();
            logger.info("Successfully created topic: {}", topicName);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error creating Kafka topic {}: {}", topicName, e.getMessage(), e);
            throw new RuntimeException("Error creating Kafka topic: " + topicName, e);
        }
    }

    // Получение детальной информации о топике
    public Map<String, TopicDescription> describeTopic(String topicName) {
        logger.info("Describing topic: {}", topicName);
        try {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topicName));
            Map<String, TopicDescription> descriptions = result.allTopicNames().get();
            logger.info("Successfully described topic: {}", topicName);
            TopicDescription description = descriptions.get(topicName);
            if (description != null) {
                logger.debug("Topic {} has {} partitions", topicName, description.partitions().size());
            }
            return descriptions;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error describing Kafka topic {}: {}", topicName, e.getMessage(), e);
            throw new RuntimeException("Error describing Kafka topic: " + topicName, e);
        }
    }

    // Удаление топика
    public void deleteTopic(String topicName) {
        logger.info("Deleting topic: {}", topicName);
        try {
            // Используем явное преобразование типа для коллекции
            Collection<String> topics = Collections.singleton(topicName);
            DeleteTopicsResult result = adminClient.deleteTopics(topics);
            result.all().get();
            logger.info("Successfully deleted topic: {}", topicName);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error deleting Kafka topic {}: {}", topicName, e.getMessage(), e);
            throw new RuntimeException("Error deleting Kafka topic: " + topicName, e);
        }
    }
}
