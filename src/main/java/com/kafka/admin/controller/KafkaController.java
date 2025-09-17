package com.kafka.admin.controller;

import com.kafka.admin.service.KafkaAdminService;
import com.kafka.admin.service.KafkaConsumerService;
import com.kafka.admin.service.KafkaProducerService;
import com.kafka.admin.util.JsonValidator;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Controller
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    private final KafkaAdminService kafkaAdminService;
    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;
    private final JsonValidator jsonValidator;

    public KafkaController(KafkaAdminService kafkaAdminService,
                           KafkaProducerService kafkaProducerService,
                           KafkaConsumerService kafkaConsumerService,
                           JsonValidator jsonValidator) {
        logger.info("Initializing KafkaController");
        this.kafkaAdminService = kafkaAdminService;
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.jsonValidator = jsonValidator;
        logger.debug("KafkaController initialized successfully");
    }

    @GetMapping("/")
    public String home() {
        logger.info("Accessing home page");
        return "index";
    }

    // Просмотр списка топиков
    @GetMapping("/topics")
    public String listTopics(Model model) {
        logger.info("Listing all Kafka topics");
        try {
            List<String> topics = kafkaAdminService.listTopics();
            logger.info("Successfully retrieved {} topics", topics.size());
            logger.debug("Topics: {}", topics);
            model.addAttribute("topics", topics);
            return "topics";
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error listing topics: {}", e.getMessage(), e);
            model.addAttribute("error", "Error listing topics: " + e.getMessage());
            return "error";
        }
    }

    // Страница создания топика
    @GetMapping("/topics/create")
    public String createTopicForm() {
        logger.info("Accessing create topic form");
        return "create-topic";
    }

    // Обработка создания топика с добавлением сообщения о результате
    @PostMapping("/topics/create")
    public String createTopic(@RequestParam String topicName,
                             @RequestParam int partitions,
                             @RequestParam short replicationFactor,
                             @RequestParam(name = "configs.keys", required = false) String[] configKeys,
                             @RequestParam(name = "configs.values", required = false) String[] configValues,
                             RedirectAttributes redirectAttributes) {
        logger.info("Creating topic: {} with {} partitions and replication factor {}",
                   topicName, partitions, replicationFactor);

        // Создаем Map для конфигураций топика
        Map<String, String> topicConfigs = new HashMap<>();

        // Проверяем, есть ли дополнительные настройки
        if (configKeys != null && configValues != null) {
            for (int i = 0; i < configKeys.length && i < configValues.length; i++) {
                if (configKeys[i] != null && !configKeys[i].isEmpty()) {
                    topicConfigs.put(configKeys[i], configValues[i]);
                }
            }
        }

        logger.debug("Topic configs: {}", topicConfigs);
        try {
            kafkaAdminService.createTopic(topicName, partitions, replicationFactor, topicConfigs);
            logger.info("Successfully created topic: {}", topicName);
            redirectAttributes.addFlashAttribute("message", "Topic created successfully");
        return "redirect:/topics";
        } catch (Exception e) {
            logger.error("Error creating topic {}: {}", topicName, e.getMessage(), e);
            redirectAttributes.addFlashAttribute("error", "Error creating topic: " + e.getMessage());
            return "redirect:/topics/create";
    }
}

    // Просмотр деталей топика
    @GetMapping("/topics/{topicName}")
    public String viewTopic(@PathVariable String topicName, Model model) {
        logger.info("Viewing topic details for: {}", topicName);
        try {
            Map<String, TopicDescription> description = kafkaAdminService.describeTopic(topicName);
            logger.debug("Retrieved topic description for: {}", topicName);
            model.addAttribute("topicName", topicName);
            model.addAttribute("description", description.get(topicName));
            return "topic-details";
        } catch (Exception e) {
            logger.error("Error viewing topic {}: {}", topicName, e.getMessage(), e);
            model.addAttribute("error", "Error viewing topic: " + e.getMessage());
            return "error";
        }
    }

    // Страница отправки сообщения
    @GetMapping("/producer")
    public String producerForm(Model model, @RequestParam(required = false) String topicName) {
        logger.info("Accessing producer form with topic: {}", topicName);
        try {
            List<String> topics = kafkaAdminService.listTopics();
            logger.debug("Retrieved {} topics for producer form", topics.size());
            model.addAttribute("topics", topics);

            // Если передан topicName, устанавливаем его как выбранный
            if (topicName != null && !topicName.isEmpty()) {
                logger.debug("Setting selected topic: {}", topicName);
                model.addAttribute("selectedTopic", topicName);
            }

            return "producer";
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error listing topics for producer form: {}", e.getMessage(), e);
            model.addAttribute("error", "Error listing topics: " + e.getMessage());
            return "error";
        }
    }

    // Отправка сообщения в топик
    @PostMapping("/producer/send")
    @ResponseBody
    public Map<String, String> sendMessage(@RequestParam String topicName,
                                         @RequestParam(required = false) String key,
                                         @RequestParam String message) {
        logger.info("Sending message to topic: {} with key: {}", topicName, key);
        logger.debug("Message content: {}", message);

        // Валидация JSON если сообщение выглядит как JSON
        if (message.trim().startsWith("{") || message.trim().startsWith("[")) {
            if (!jsonValidator.isValidJson(message)) {
                logger.warn("Invalid JSON format in message");
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
                response.put("message", "Invalid JSON format");
            return response;
        }
            logger.debug("Message validated as valid JSON");
    }

        try {
            kafkaProducerService.sendMessage(topicName, key, message);
            logger.info("Successfully sent message to topic: {}", topicName);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Message sent to topic " + topicName);
            return response;
        } catch (Exception e) {
            logger.error("Error sending message to topic {}: {}", topicName, e.getMessage(), e);
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
        }
    }

    // Страница чтения сообщений
    @GetMapping("/consumer")
    public String consumerForm(Model model, @RequestParam(required = false) String topicName) {
        logger.info("Accessing consumer form with topic: {}", topicName);
        try {
            List<String> topics = kafkaAdminService.listTopics();
            logger.debug("Retrieved {} topics for consumer form", topics.size());
            model.addAttribute("topics", topics);

            // Если передан topicName, устанавливаем его как выбранный
            if (topicName != null && !topicName.isEmpty()) {
                logger.debug("Setting selected topic: {}", topicName);
                model.addAttribute("selectedTopic", topicName);
            }

            return "consumer";
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error listing topics for consumer form: {}", e.getMessage(), e);
            model.addAttribute("error", "Error listing topics: " + e.getMessage());
            return "error";
        }
    }

    // Чтение сообщений из топика
    @PostMapping("/consumer/read")
    @ResponseBody
    public Map<String, Object> readMessages(@RequestParam String topicName,
                                          @RequestParam String groupId,
                                          @RequestParam(defaultValue = "10") int maxMessages,
                                          @RequestParam(defaultValue = "5000") long pollTimeoutMs,
                                          @RequestParam(defaultValue = "true") boolean fromBeginning) {
        logger.info("Reading messages from topic: {}, groupId: {}, maxMessages: {}, pollTimeout: {}, fromBeginning: {}",
                   topicName, groupId, maxMessages, pollTimeoutMs, fromBeginning);
        try {
            List<Map<String, Object>> messages =
                kafkaConsumerService.consumeMessages(topicName, groupId, maxMessages, pollTimeoutMs, fromBeginning);
            logger.info("Successfully read {} messages from topic: {}", messages.size(), topicName);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("messages", messages);
            response.put("count", messages.size());
            return response;
        } catch (Exception e) {
            logger.error("Error reading messages from topic {}: {}", topicName, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
        }
    }

    // Чтение из конкретной партиции и смещения
    @PostMapping("/consumer/read-specific")
    @ResponseBody
    public Map<String, Object> readFromPartitionAndOffset(@RequestParam String topicName,
                                                       @RequestParam int partition,
                                                       @RequestParam long offset,
                                                       @RequestParam(defaultValue = "10") int maxMessages) {
        logger.info("Reading messages from topic: {}, partition: {}, offset: {}, maxMessages: {}",
                   topicName, partition, offset, maxMessages);
        try {
            List<Map<String, Object>> messages =
                kafkaConsumerService.consumeFromPartitionAndOffset(topicName, partition, offset, maxMessages);
            logger.info("Successfully read {} messages from topic: {} partition: {} starting at offset: {}",
                       messages.size(), topicName, partition, offset);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("messages", messages);
            response.put("count", messages.size());
            return response;
        } catch (Exception e) {
            logger.error("Error reading messages from topic {} partition {} offset {}: {}",
                        topicName, partition, offset, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
}
    }

    // Удаление топика
    @PostMapping("/topics/{topicName}/delete")
    public String deleteTopic(@PathVariable String topicName) {
        logger.info("Deleting topic: {}", topicName);
        try {
            kafkaAdminService.deleteTopic(topicName);
            logger.info("Successfully deleted topic: {}", topicName);
        } catch (Exception e) {
            logger.error("Error deleting topic {}: {}", topicName, e.getMessage(), e);
        }
        return "redirect:/topics";
    }
}
