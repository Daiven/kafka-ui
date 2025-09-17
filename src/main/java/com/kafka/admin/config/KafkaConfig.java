package com.kafka.admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Конфигурация для Kafka Admin Client
    @Bean
    public KafkaAdmin kafkaAdmin() {
        logger.info("Creating KafkaAdmin bean with bootstrap servers: {}", bootstrapServers);
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        logger.debug("KafkaAdmin configuration: {}", configs);
        return new KafkaAdmin(configs);
    }
    // Фабрика для администрирования Kafka
    @Bean
    public AdminClient adminClient() {
        logger.info("Creating AdminClient bean");
        AdminClient client = AdminClient.create(kafkaAdmin().getConfigurationProperties());
        logger.debug("AdminClient created successfully");
        return client;
    }
    // Конфигурация для Kafka Producer
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        logger.info("Creating ProducerFactory bean with bootstrap servers: {}", bootstrapServers);
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        logger.debug("Producer configuration: {}", configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        logger.info("Creating KafkaTemplate bean");
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
        logger.debug("KafkaTemplate created successfully");
        return template;
    }

    // Конфигурация для Kafka Consumer
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        logger.info("Creating ConsumerFactory bean with bootstrap servers: {}", bootstrapServers);
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        logger.debug("Consumer configuration: {}", props);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        logger.info("Creating ConcurrentKafkaListenerContainerFactory bean");
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        logger.debug("KafkaListenerContainerFactory created successfully");
        return factory;
    }
}