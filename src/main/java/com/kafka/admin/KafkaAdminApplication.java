package com.kafka.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaAdminApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Admin Application");
        try {
        SpringApplication.run(KafkaAdminApplication.class, args);
            logger.info("Kafka Admin Application started successfully");
        } catch (Exception e) {
            logger.error("Failed to start Kafka Admin Application: {}", e.getMessage(), e);
            throw e;
    }
}
}
