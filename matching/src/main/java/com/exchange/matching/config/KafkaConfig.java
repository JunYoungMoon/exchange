package com.exchange.matching.config;

import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 특정 이벤트 타입에 대한 Kafka 설정 클래스
 */
@Configuration
public class KafkaConfig {

    private static final int DEFAULT_CONCURRENCY = 3;
    private static final int RECOVERY_CONCURRENCY = 2;

    private final KafkaCommonConfig kafkaCommonConfig;

    public KafkaConfig(KafkaCommonConfig kafkaCommonConfig) {
        this.kafkaCommonConfig = kafkaCommonConfig;
    }

    /**
     * 매칭 이벤트 리스너 컨테이너 팩토리
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMatchingEvent> orderDeliveryKafkaListenerContainerFactory() {
        return kafkaCommonConfig.createAutoCommitListenerFactory(
                new TypeReference<>() {
                }, "matching-service", DEFAULT_CONCURRENCY);
    }

    /**
     * 복구 이벤트 리스너 컨테이너 팩토리
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> recoveryEventKafkaListenerContainerFactory() {
        return kafkaCommonConfig.createManualCommitListenerFactory(
                new TypeReference<>() {
                }, "recovery-service", RECOVERY_CONCURRENCY);
    }

    /**
     * 매칭 이벤트 전용 Kafka 템플릿
     */
    @Bean
    public KafkaTemplate<String, KafkaMatchingEvent> orderDeliveryKafkaTemplate() {
        KafkaTemplate<String, KafkaMatchingEvent> kafkaTemplate = new KafkaTemplate<>(
                kafkaCommonConfig.createCustomProducerFactory(new TypeReference<>() {
                }));

//        kafkaTemplate.setObservationEnabled(true);
        return kafkaTemplate;
    }

    /**
     * 제네릭 객체 전송용 Kafka 템플릿
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(
                kafkaCommonConfig.createCustomProducerFactory(new TypeReference<>() {
                }));
    }

    /**
     * 장애시 재시도 Kafka 템플릿
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMatchingEvent> delayedRetryListenerContainerFactory() {
        return kafkaCommonConfig.createManualCommitListenerFactory(
                new TypeReference<>() {
                }, "retry-processor", DEFAULT_CONCURRENCY);
    }
}