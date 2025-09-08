package com.example.kafkaui.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${kafka.bootstrapServers:localhost:9092}")
    private String bootstrapServers;

    public KafkaService(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public List<String> listTopics(String overrideBootstrap) throws ExecutionException, InterruptedException {
        String servers = overrideBootstrap != null && !overrideBootstrap.isBlank() ? overrideBootstrap : bootstrapServers;
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers))) {
            return new ArrayList<>(admin.listTopics().names().get());
        }
    }

    public void send(String serversOverride,
                     String topic,
                     String key,
                     Integer partition,
                     Map<String, String> headers,
                     byte[] value) {
        if (serversOverride != null && !serversOverride.isBlank()) {
            // create ad-hoc template with override servers
            KafkaTemplate<String, byte[]> tempTemplate = KafkaTemplateFactory.create(serversOverride);
            doSend(tempTemplate, topic, key, partition, headers, value);
        } else {
            doSend(this.kafkaTemplate, topic, key, partition, headers, value);
        }
    }

    private void doSend(KafkaTemplate<String, byte[]> template,
                        String topic,
                        String key,
                        Integer partition,
                        Map<String, String> headers,
                        byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, key, value);
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v.getBytes()));
        }
        template.send(record);
    }

    public List<ConsumerRecord<String, byte[]>> consumeOnce(
            String serversOverride,
            String topic,
            String groupId,
            String offsetReset,
            Integer partition,
            Long offset,
            Integer maxRecords,
            Long pollMillis
    ) {
        String servers = serversOverride != null && !serversOverride.isBlank() ? serversOverride : bootstrapServers;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null && !groupId.isBlank() ? groupId : "kafka-ui-temp-");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset == null || offsetReset.isBlank() ? "latest" : offsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            if (partition != null) {
                TopicPartition tp = new TopicPartition(topic, partition);
                consumer.assign(Collections.singletonList(tp));
                if (offset != null) {
                    consumer.seek(tp, offset);
                }
            } else {
                consumer.subscribe(Collections.singletonList(topic));
            }

            List<ConsumerRecord<String, byte[]>> result = new ArrayList<>();
            long deadline = System.currentTimeMillis() + (pollMillis != null ? pollMillis : 2000);
            int limit = maxRecords != null && maxRecords > 0 ? maxRecords : 50;

            while (System.currentTimeMillis() < deadline && result.size() < limit) {
                consumer.poll(Duration.ofMillis(200)).forEach(result::add);
            }

            return result;
        }
    }

    private static class KafkaTemplateFactory {
        static KafkaTemplate<String, byte[]> create(String servers) {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
            ProducerFactory<String, byte[]> pf = new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(configProps);
            return new KafkaTemplate<>(pf);
        }
    }
}



