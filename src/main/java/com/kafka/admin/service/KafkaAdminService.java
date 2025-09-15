package com.kafka.admin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaAdminService {

    private final KafkaAdmin kafkaAdmin;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    // Добавляем этот метод для поддержки тестирования
    protected AdminClient createAdminClient() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    // Получение списка топиков
    public List<String> listTopics() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = createAdminClient()) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topicNames = listTopicsResult.names().get();
            return new ArrayList<>(topicNames);
        }
    }

    // Создание нового топика
    public void createTopic(String topicName, int numPartitions, short replicationFactor,
                           Map<String, String> configs) {
        try (AdminClient adminClient = createAdminClient()) {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            if (configs != null && !configs.isEmpty()) {
                newTopic.configs(configs);
            }

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            // Ждем завершения операции
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error creating Kafka topic: " + topicName, e);
        }
    }

    // Получение детальной информации о топике
    public Map<String, TopicDescription> describeTopic(String topicName) {
        try (AdminClient adminClient = createAdminClient()) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topicName));
            return result.allTopicNames().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error describing Kafka topic: " + topicName, e);
        }
    }

    // Удаление топика
    public void deleteTopic(String topicName) {
        try (AdminClient adminClient = createAdminClient()) {
            // Используем явное преобразование типа для коллекции
            Collection<String> topics = Collections.singleton(topicName);
            DeleteTopicsResult result = adminClient.deleteTopics(topics);
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error deleting Kafka topic: " + topicName, e);
        }
    }
}