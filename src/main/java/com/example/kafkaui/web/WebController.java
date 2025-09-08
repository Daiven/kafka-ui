package com.example.kafkaui.web;

import com.example.kafkaui.service.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Controller
@RequestMapping("/")
public class WebController {

    private final KafkaService kafkaService;

    public WebController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @GetMapping
    public String index() {
        return "index";
    }

    @GetMapping("/topics")
    public String topics(@RequestParam(value = "bootstrap", required = false) String bootstrap,
                         Model model) throws ExecutionException, InterruptedException {
        List<String> topics = kafkaService.listTopics(bootstrap);
        Collections.sort(topics);
        model.addAttribute("topics", topics);
        model.addAttribute("bootstrap", bootstrap);
        return "index";
    }

    @PostMapping("/produce")
    public String produce(@RequestParam(value = "bootstrap", required = false) String bootstrap,
                          @RequestParam("topic") String topic,
                          @RequestParam(value = "key", required = false) String key,
                          @RequestParam(value = "partition", required = false) Integer partition,
                          @RequestParam(value = "headers", required = false) String headers,
                          @RequestParam(value = "value", required = false) String value,
                          @RequestParam(value = "file", required = false) MultipartFile file,
                          Model model) throws Exception {
        Map<String, String> headerMap = parseHeaders(headers);
        byte[] payload;
        if (file != null && !file.isEmpty()) {
            payload = file.getBytes();
        } else {
            payload = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        }
        kafkaService.send(bootstrap, topic, key, partition, headerMap, payload);
        model.addAttribute("produceOk", true);
        return index();
    }

    @GetMapping("/consume")
    public String consume(@RequestParam(value = "bootstrap", required = false) String bootstrap,
                          @RequestParam("topic") String topic,
                          @RequestParam(value = "groupId", required = false) String groupId,
                          @RequestParam(value = "offsetReset", required = false) String offsetReset,
                          @RequestParam(value = "partition", required = false) Integer partition,
                          @RequestParam(value = "offset", required = false) Long offset,
                          @RequestParam(value = "max", required = false) Integer max,
                          @RequestParam(value = "timeout", required = false) Long timeout,
                          Model model) {
        List<ConsumerRecord<String, byte[]>> records = kafkaService.consumeOnce(
                bootstrap, topic, groupId, offsetReset, partition, offset, max, timeout);
        model.addAttribute("consumed", records);
        return "index";
    }

    private Map<String, String> parseHeaders(String headers) {
        Map<String, String> map = new LinkedHashMap<>();
        if (!StringUtils.hasText(headers)) return map;
        String[] pairs = headers.split(",");
        for (String p : pairs) {
            int idx = p.indexOf('=');
            if (idx > 0 && idx < p.length() - 1) {
                map.put(p.substring(0, idx).trim(), p.substring(idx + 1).trim());
            }
        }
        return map;
    }
}



