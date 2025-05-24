package com.exchange.matching.config;

import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.matching.util.CustomJsonDeserializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class ReactiveKafkaConfig {

    private final KafkaCommonConfig kafkaCommonConfig;
    private final ObjectMapper objectMapper;

    private static final List<String> ORDER_DELIVERY_TOPICS = Arrays.asList(
            "user-to-matching.execute-order-delivery.v1a",
            "user-to-matching.execute-order-delivery.v1b",
            "user-to-matching.execute-order-delivery.v2",
            "user-to-matching.execute-order-delivery.v3",
            "user-to-matching.execute-order-delivery.v4",
            "user-to-matching.execute-order-delivery.v5",
            "user-to-matching.execute-order-delivery.v6a",
            "user-to-matching.execute-order-delivery.v6b",
            "user-to-matching.execute-order-delivery.v6c",
            "user-to-matching.execute-order-delivery.v6d"
    );

    public ReactiveKafkaConfig(KafkaCommonConfig kafkaCommonConfig, ObjectMapper objectMapper) {
        this.kafkaCommonConfig = kafkaCommonConfig;
        this.objectMapper = objectMapper;
    }

    @Bean
    public ReceiverOptions<String, KafkaMatchingEvent> kafkaReceiverOptions() {
        Map<String, Object> props = kafkaCommonConfig.getAutoCommitConsumerConfig("matching-service");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        return ReceiverOptions.<String, KafkaMatchingEvent>create(props)
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new CustomJsonDeserializer<>(objectMapper, new TypeReference<>() {
                }))
                .subscription(ORDER_DELIVERY_TOPICS);
    }

    @Bean
    public KafkaReceiver<String, KafkaMatchingEvent> kafkaReceiver(ReceiverOptions<String, KafkaMatchingEvent> receiverOptions) {
        return KafkaReceiver.create(receiverOptions);
    }
}