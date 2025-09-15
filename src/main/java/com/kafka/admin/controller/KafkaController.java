package com.kafka.admin.controller;

import com.kafka.admin.service.KafkaAdminService;
import com.kafka.admin.service.KafkaConsumerService;
import com.kafka.admin.service.KafkaProducerService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Controller
public class KafkaController {

    private final KafkaAdminService kafkaAdminService;
    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;

    public KafkaController(KafkaAdminService kafkaAdminService,
                           KafkaProducerService kafkaProducerService,
                           KafkaConsumerService kafkaConsumerService) {
        this.kafkaAdminService = kafkaAdminService;
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaConsumerService = kafkaConsumerService;
    }

    @GetMapping("/")
    public String home() {
        return "index";
    }

    // Просмотр списка топиков
    @GetMapping("/topics")
    public String listTopics(Model model) {
        try {
            List<String> topics = kafkaAdminService.listTopics();
            model.addAttribute("topics", topics);
            return "topics";
        } catch (ExecutionException | InterruptedException e) {
            model.addAttribute("error", "Error listing topics: " + e.getMessage());
            return "error";
        }
    }

    // Страница создания топика
    @GetMapping("/topics/create")
    public String createTopicForm() {
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

        try {
            kafkaAdminService.createTopic(topicName, partitions, replicationFactor, topicConfigs);
            redirectAttributes.addFlashAttribute("message", "Topic created successfully");
        return "redirect:/topics";
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Error creating topic: " + e.getMessage());
            return "redirect:/topics/create";
    }
}

    // Просмотр деталей топика
    @GetMapping("/topics/{topicName}")
    public String viewTopic(@PathVariable String topicName, Model model) {
        Map<String, TopicDescription> description = kafkaAdminService.describeTopic(topicName);

        model.addAttribute("topicName", topicName);
        model.addAttribute("description", description.get(topicName));
        return "topic-details";
    }

    // Страница отправки сообщения
    @GetMapping("/producer")
    public String producerForm(Model model, @RequestParam(required = false) String topicName) {
        try {
            List<String> topics = kafkaAdminService.listTopics();
            model.addAttribute("topics", topics);

            // Если передан topicName, устанавливаем его как выбранный
            if (topicName != null && !topicName.isEmpty()) {
                model.addAttribute("selectedTopic", topicName);
            }

            return "producer";
        } catch (ExecutionException | InterruptedException e) {
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
        try {
            kafkaProducerService.sendMessage(topicName, key, message);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Message sent to topic " + topicName);
            return response;
        } catch (Exception e) {
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
        }
    }

    // Страница чтения сообщений
    @GetMapping("/consumer")
    public String consumerForm(Model model, @RequestParam(required = false) String topicName) {
        try {
            List<String> topics = kafkaAdminService.listTopics();
            model.addAttribute("topics", topics);

            // Если передан topicName, устанавливаем его как выбранный
            if (topicName != null && !topicName.isEmpty()) {
                model.addAttribute("selectedTopic", topicName);
            }

            return "consumer";
        } catch (ExecutionException | InterruptedException e) {
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
        try {
            List<Map<String, Object>> messages =
                kafkaConsumerService.consumeMessages(topicName, groupId, maxMessages, pollTimeoutMs, fromBeginning);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("messages", messages);
            response.put("count", messages.size());
            return response;
        } catch (Exception e) {
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
        try {
            List<Map<String, Object>> messages =
                kafkaConsumerService.consumeFromPartitionAndOffset(topicName, partition, offset, maxMessages);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("messages", messages);
            response.put("count", messages.size());
            return response;
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
        }
    }

    // Удаление топика
    @PostMapping("/topics/{topicName}/delete")
    public String deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return "redirect:/topics";
    }
}
