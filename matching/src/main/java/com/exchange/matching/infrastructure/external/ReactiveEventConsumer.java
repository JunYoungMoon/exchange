package com.exchange.matching.infrastructure.external;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.service.MatchingApplicationService;
import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.matching.util.MetricsCollector;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveEventConsumer {
    private final MatchingApplicationService matchingService;
    private final MetricsCollector metricsCollector;
    private final KafkaReceiver<String, KafkaMatchingEvent> kafkaReceiver;
    private final ReceiveServerHealthMonitor healthMonitor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong memoryUsageMB = new AtomicLong(0);

    @Value("${kafka.consumer.retry-topic:matching-to-matching.execute-receiver-unavailable.retry}")
    private String retryTopic;

    @Value("${kafka.consumer.memory-threshold-mb:1024}")
    private long memoryThresholdMB;

    private final AtomicInteger consecutiveErrors = new AtomicInteger(0);
    private final AtomicLong lastErrorTime = new AtomicLong(0);
    private static final int MAX_CONSECUTIVE_ERRORS = 5;
    private static final long CIRCUIT_BREAKER_TIMEOUT = 30000; // 30초

    @EventListener(ApplicationStartedEvent.class)
    public void startConsumer() {
        kafkaReceiver.receive()
                .publishOn(Schedulers.boundedElastic())
                .filter(record -> {
                    if (!running.get()) {
                        record.receiverOffset().acknowledge();
                        return false;
                    }

                    // 메모리 사용량 체크 및 속도 조절
                    updateMemoryUsage();
                    if (isMemoryPressureHigh()) {
                        pauseForMemoryRecovery();
                    }

                    // 서킷 브레이크
                    if (isCircuitBreakerOpen()) {
                        log.warn("Circuit Breaker 열림 - 처리 중단: Offset={}", record.offset());
                        return false;
                    }

                    return true;
                })
                .flatMap(this::processRecordWithErrorHandling)
                .subscribe(
                        success -> {},
                        error -> {
                            log.error("Kafka 스트림 처리 오류", error);
                            restartConsumerAfterDelay();
                        },
                        () -> log.info("Kafka 스트림 처리 완료")
                );
    }

    private Mono<Void> processRecordWithErrorHandling(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return processRecord(record)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                        .maxBackoff(Duration.ofSeconds(2))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal ->
                                log.warn("레코드 재시도 - Offset: {}, 시도: {}/3",
                                        record.offset(), retrySignal.totalRetries() + 1)))

                .onErrorResume(error -> handleRecordFailure(record, error))

                .doOnSuccess(v -> consecutiveErrors.set(0))
                .doOnError(this::incrementConsecutiveErrors);
    }

    private Mono<Void> processRecord(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.defer(() -> {
                    // 헬스 체크
                    if (!healthMonitor.isHealthy()) {
                        log.warn("Receive 서버 상태 이상 - 재시도 토픽으로 전송: Offset={}", record.offset());
                        return sendToRetryTopic(record, "Receive 서버 상태 이상")
                                .then(acknowledgeRecord(record));
                    }

                    // 메시지 처리
                    try {
                        String topic = record.topic();
                        MatchingVersion version = extractVersionFromTopic(topic);
                        KafkaMatchingEvent event = record.value();
                        CreateMatchingCommand command = KafkaMatchingEvent.commandFromEvent(event);

                        return metricsCollector.recordProcessingReactive(() ->
                                        matchingService.processMatchingReactive(command, version), version)
                                .then(acknowledgeRecord(record));

                    } catch (Exception e) {
                        log.error("레코드 파싱 실패 - Offset: {}", record.offset(), e);
                        return Mono.error(new ProcessingException("레코드 파싱 실패", e));
                    }
                })
                .timeout(Duration.ofSeconds(30));
    }

    private Mono<Void> acknowledgeRecord(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.fromRunnable(() -> record.receiverOffset().acknowledge());
    }

    private Mono<Void> sendToRetryTopic(ReceiverRecord<String, KafkaMatchingEvent> record, String reason) {
        return Mono.fromRunnable(() -> {
            try {
                RetryMessage retryMessage = RetryMessage.builder()
                        .originalTopic(record.topic())
                        .originalPartition(record.partition())
                        .originalOffset(record.offset())
                        .originalKey(record.key())
                        .originalMessage(record.value())
                        .failureReason(reason)
                        .retryTime(Instant.now())
                        .build();

                kafkaTemplate.send(retryTopic, record.key(), retryMessage);

            } catch (Exception e) {
                log.error("재시도 토픽 전송 중 오류", e);
            }
        });
    }

    private Mono<Void> handleRecordFailure(ReceiverRecord<String, KafkaMatchingEvent> record, Throwable error) {
        log.error("레코드 최종 실패 - 재시도 토픽으로 전송: Offset={}", record.offset(), error);

        return sendToRetryTopic(record, "최종 처리 실패: " + error.getMessage())
                .then(acknowledgeRecord(record));
    }

    private boolean isRetryableError(Throwable throwable) {
        if (throwable instanceof ProcessingException) {
            return true;
        }
        if (throwable.getCause() instanceof java.sql.SQLException) {
            return true;
        }
        if (throwable instanceof java.util.concurrent.TimeoutException) {
            return true;
        }
        return false;
    }

    private void incrementConsecutiveErrors(Throwable ignoredError) {
        int errorCount = consecutiveErrors.incrementAndGet();
        lastErrorTime.set(System.currentTimeMillis());

        if (errorCount >= MAX_CONSECUTIVE_ERRORS) {
            log.error("Circuit Breaker 임계값 도달 - {}초 후 재시도", CIRCUIT_BREAKER_TIMEOUT / 1000);
        }
    }

    private boolean isCircuitBreakerOpen() {
        if (consecutiveErrors.get() >= MAX_CONSECUTIVE_ERRORS) {
            long timeSinceLastError = System.currentTimeMillis() - lastErrorTime.get();
            if (timeSinceLastError > CIRCUIT_BREAKER_TIMEOUT) {
                consecutiveErrors.set(0);
                return false;
            }
            return true;
        }
        return false;
    }

    private void restartConsumerAfterDelay() {
        Mono.delay(Duration.ofSeconds(5))
                .subscribe(tick -> {
                    if (consecutiveErrors.get() < MAX_CONSECUTIVE_ERRORS) {
                        startConsumer();
                    }
                });
    }

    private MatchingVersion extractVersionFromTopic(String topic) {
        String versionCode = topic.substring(topic.lastIndexOf('.') + 1);
        return MatchingVersion.fromCode(versionCode);
    }

    private void updateMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
        memoryUsageMB.set(usedMemory);
    }

    private boolean isMemoryPressureHigh() {
        return memoryUsageMB.get() > memoryThresholdMB;
    }

    private void pauseForMemoryRecovery() {
        long currentMemory = memoryUsageMB.get();
        long maxMemory = Runtime.getRuntime().maxMemory() / 1024 / 1024;
        double usagePercent = (double) currentMemory / maxMemory * 100;

        log.warn("메모리 사용률: {}% ({}MB / {}MB)", usagePercent, currentMemory, maxMemory);

        try {
            // 메모리 사용률에 따른 단계적 지연
            if (usagePercent > 85) {
                Thread.sleep(500); // Critical: 0.5초 지연
            } else if (usagePercent > 75) {
                Thread.sleep(200); // Warning: 0.2초 지연
            } else {
                Thread.sleep(100); // Info: 0.1초 지연
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class ProcessingException extends RuntimeException {
        public ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Builder
    @Data
    public static class RetryMessage {
        private String originalTopic;
        private int originalPartition;
        private long originalOffset;
        private String originalKey;
        private KafkaMatchingEvent originalMessage;
        private String failureReason;
        private Instant retryTime;
    }
}