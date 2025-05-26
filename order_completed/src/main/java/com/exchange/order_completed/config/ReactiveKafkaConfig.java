package com.exchange.order_completed.config;

import com.exchange.order_completed.infrastructure.dto.KafkaMatchedOrderStoreEvent;
import com.exchange.order_completed.util.CustomJsonDeserializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class ReactiveKafkaConfig {

    @Value("${spring.kafka.host}")
    private String kafkaHost;

    @Value("${spring.kafka.username}")
    private String kafkaUsername;

    @Value("${spring.kafka.password}")
    private String kafkaPassword;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    private final ObjectMapper objectMapper;

    public ReactiveKafkaConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Reactor Kafka Receiver 설정 (Consumer)
     */
    @Bean
    public ReceiverOptions<String, KafkaMatchedOrderStoreEvent> receiverOptions() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "matching-service");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return ReceiverOptions.<String, KafkaMatchedOrderStoreEvent>create(consumerProps)
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new CustomJsonDeserializer<>(objectMapper, new TypeReference<>() {
                }))
                .subscription(List.of("matching-to-order_completed.execute-order-matched"));
    }

    @Bean
    public KafkaReceiver<String, KafkaMatchedOrderStoreEvent> kafkaReceiver(ReceiverOptions<String, KafkaMatchedOrderStoreEvent> receiverOptions) {
        return KafkaReceiver.create(receiverOptions);
    }

    /**
     * SASL 인증 설정 추가 (Consumer/Producer 공통)
     */
    private void addSaslAuthConfig(Map<String, Object> props) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + kafkaUsername + "\" password=\"" + kafkaPassword + "\";");
    }
}
