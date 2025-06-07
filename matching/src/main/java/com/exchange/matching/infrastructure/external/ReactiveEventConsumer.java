package com.exchange.matching.infrastructure.external;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.service.MatchingApplicationService;
import com.exchange.matching.common.exception.RedisBackpressureException;
import com.exchange.matching.common.exception.RedisCircuitBreakerException;
import com.exchange.matching.infrastructure.dto.DLQMessage;
import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.matching.util.MetricsCollector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveEventConsumer {
    private final MatchingApplicationService matchingService;
    private final MetricsCollector metricsCollector;
    private final KafkaReceiver<String, KafkaMatchingEvent> kafkaReceiver;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.consumer.dlq-topic:matching-to-matching.execute-receiver-unavailable.dlq}")
    private String dlqTopic;

    @Value("${kafka.consumer.max-retry-count:3}")
    private int maxRetryCount;

    // 서킷브레이커 관련
    private final AtomicInteger consecutiveErrors = new AtomicInteger(0);
    private final AtomicLong lastErrorTime = new AtomicLong(0);
    private static final int MAX_CONSECUTIVE_ERRORS = 5;
    private static final long CIRCUIT_BREAKER_TIMEOUT = 30000; // 30초

    // 하이브리드 방식을 위한 추가 필드들
    private final Semaphore dynamicLimiter = new Semaphore(4); // 동적 제어용
    private final AtomicInteger currentLimiterPermits = new AtomicInteger(4);

    // 메모리 모니터링 추가
    private final AtomicLong lastMemoryCheck = new AtomicLong(0);
    private volatile boolean isMemoryPressureMode = false;

    // 헤더 상수
    private static final String RETRY_COUNT_HEADER = "X-Retry-Count";
    private static final String ORIGINAL_TOPIC_HEADER = "X-Original-Topic";
    private static final String FAILURE_REASON_HEADER = "X-Failure-Reason";
    private static final String DELAY_REASON_HEADER = "X-Delay-Reason";
    private static final String RETRY_TIMESTAMP_HEADER = "X-Retry-Timestamp";

    @EventListener(ApplicationStartedEvent.class)
    public void startConsumer() {
        kafkaReceiver.receive()
                .publishOn(Schedulers.boundedElastic())
                .flatMap(this::processRecordWithHybridControl)
                .subscribe(
                        success -> {},
                        error -> {
                            log.error("Kafka 스트림 처리 오류", error);
                            restartConsumerAfterDelay();
                        },
                        () -> log.info("Kafka 스트림 처리 완료")
                );
    }

    /**
     * 하이브리드 제어: 메모리 압박시만 추가 제어 적용
     */
    private Mono<Void> processRecordWithHybridControl(ReceiverRecord<String, KafkaMatchingEvent> record) {
        // 주기적 메모리 체크 (성능을 위해 1초마다)
        checkMemoryPressurePeriodically();

        if (isMemoryPressureMode) {
            // 메모리 압박시: Semaphore로 추가 제어
            return processWithMemoryControl(record);
        } else {
            // 메모리 여유시: 바로 처리
            return processRecordSafely(record);
        }
    }

    /**
     * 메모리 압박시 세마포어 제어 처리
     */
    private Mono<Void> processWithMemoryControl(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.fromCallable(() -> {
                    // 논블로킹 시도 먼저
                    if (dynamicLimiter.tryAcquire()) {
                        return true;
                    }

                    // 실패시 블로킹 acquire (별도 스레드에서)
                    dynamicLimiter.acquire();
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic()) // acquire 블로킹을 별도 스레드에서
                .flatMap(acquired -> processRecordSafely(record))
                .doFinally(signal -> {
                    dynamicLimiter.release();
                    log.debug("메모리 제어 모드 처리 완료: OrderId={}",
                            record.value().getOrderId());
                });
    }

    /**
     * 주기적 메모리 압박 상태 체크 (1초마다)
     */
    private void checkMemoryPressurePeriodically() {
        long now = System.currentTimeMillis();
        if (now - lastMemoryCheck.get() > 1000) { // 1초마다 체크
            lastMemoryCheck.set(now);

            boolean newPressureState = isMemoryPressureHigh();
            if (newPressureState != isMemoryPressureMode) {
                isMemoryPressureMode = newPressureState;
                log.info("메모리 압박 모드 변경: {} (사용률: {}%)",
                        isMemoryPressureMode ? "ON" : "OFF",
                        getMemoryUsagePercent());
            }
        }
    }

    /**
     * 메모리 압박 상태 확인
     */
    private boolean isMemoryPressureHigh() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        double usagePercent = (double) usedMemory / maxMemory;
        return usagePercent > 0.75; // 75% 이상시 압박 상태
    }

    /**
     * 현재 메모리 사용률 조회
     */
    private double getMemoryUsagePercent() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        return (double) usedMemory / maxMemory * 100;
    }

    /**
     * 동적 세마포어 조절 (스케줄링)
     */
    @Scheduled(fixedDelay = 10000) // 10초마다
    public void adjustDynamicLimiter() {
        if (!isMemoryPressureMode) {
            return; // 메모리 여유시 조절 불필요
        }

        double memoryUsage = getMemoryUsagePercent();
        int optimalPermits = calculateOptimalPermits(memoryUsage);
        int currentPermits = currentLimiterPermits.get();

        if (optimalPermits != currentPermits) {
            adjustSemaphorePermits(currentPermits, optimalPermits);
            currentLimiterPermits.set(optimalPermits);

            log.info("메모리 압박 모드 - 동시성 조절: {} → {} (메모리: {}%)",
                    currentPermits, optimalPermits, memoryUsage);
        }
    }

    /**
     * 메모리 사용률에 따른 최적 허가 수 계산
     */
    private int calculateOptimalPermits(double memoryUsagePercent) {
        if (memoryUsagePercent > 90) {
            return 1; // 위험: 순차 처리
        } else if (memoryUsagePercent > 85) {
            return 2; // 높음: 최소 병렬
        } else if (memoryUsagePercent > 80) {
            return 3; // 중간: 제한적 병렬
        } else {
            return 4; // 보통: 일반 병렬
        }
    }

    /**
     * 세마포어 허가 수 조절
     */
    private void adjustSemaphorePermits(int current, int target) {
        if (target > current) {
            // 허가 증가
            int increase = target - current;
            dynamicLimiter.release(increase);
            log.debug("세마포어 허가 증가: +{}", increase);

        } else if (target < current) {
            // 허가 감소 (논블로킹 방식)
            int decrease = current - target;
            int actualDecrease = 0;

            for (int i = 0; i < decrease; i++) {
                if (dynamicLimiter.tryAcquire()) {
                    actualDecrease++;
                } else {
                    break; // 더 이상 회수할 수 없음
                }
            }

            if (actualDecrease > 0) {
                log.debug("세마포어 허가 감소: -{}", actualDecrease);
            }
        }
    }

    /**
     * 현재 시스템 상태 모니터링 로깅
     */
    @Scheduled(fixedDelay = 30000) // 30초마다
    public void logSystemStatus() {
        if (log.isInfoEnabled()) {
            double memoryUsage = getMemoryUsagePercent();
            int availablePermits = dynamicLimiter.availablePermits();
            int usedPermits = currentLimiterPermits.get() - availablePermits;

            log.info("시스템 상태 - 메모리: {}%, 압박모드: {}, 동시처리: {}/{}, 대기중: {}",
                    String.format("%.1f", memoryUsage),
                    isMemoryPressureMode ? "ON" : "OFF",
                    usedPermits,
                    currentLimiterPermits.get(),
                    dynamicLimiter.getQueueLength());
        }
    }

    /**
     * 레코드 안전 처리 (서킷브레이커 포함)
     */
    private Mono<Void> processRecordSafely(ReceiverRecord<String, KafkaMatchingEvent> record) {
        // 서킷브레이커 체크
        if (isCircuitBreakerOpen()) {
            log.warn("Circuit Breaker 열림 - 지연 토픽 전송: OrderId={}",
                    record.value().getOrderId());
            return handleCircuitBreakerFailure(record);
        }

        // 정상 처리
        return processRecordWithErrorHandling(record);
    }

    /**
     * 에러 핸들링을 포함한 레코드 처리
     */
    private Mono<Void> processRecordWithErrorHandling(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return processRecord(record)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                        .maxBackoff(Duration.ofSeconds(2))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal ->
                                log.warn("레코드 재시도 - OrderId: {}, 시도: {}/3",
                                        record.value().getOrderId(), retrySignal.totalRetries() + 1)))
                .onErrorResume(error -> handleRecordFailure(record, error))
                .doOnSuccess(v -> consecutiveErrors.set(0))
                .doOnError(this::incrementConsecutiveErrors);
    }

    /**
     * 실제 비즈니스 로직 처리
     */
    private Mono<Void> processRecord(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.defer(() -> {
                    try {
                        String topic = record.topic();
                        MatchingVersion version = extractVersionFromTopic(topic);
                        KafkaMatchingEvent event = record.value();
                        CreateMatchingCommand command = KafkaMatchingEvent.commandFromEvent(event);

                        return metricsCollector.recordProcessingReactive(() ->
                                        matchingService.processMatchingReactive(command, version), version)
                                .then(acknowledgeRecord(record));

                    } catch (Exception e) {
                        log.error("레코드 파싱 실패 - OrderId: {}", record.value().getOrderId(), e);
                        return Mono.error(new ProcessingException("레코드 파싱 실패", e));
                    }
                })
                .timeout(Duration.ofSeconds(30));
    }

    /**
     * 레코드 ACK 처리
     */
    private Mono<Void> acknowledgeRecord(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.fromRunnable(() -> record.receiverOffset().acknowledge());
    }

    /**
     * 레코드 처리 실패 핸들링
     */
    private Mono<Void> handleRecordFailure(ReceiverRecord<String, KafkaMatchingEvent> record, Throwable error) {
        return handleProcessingFailure(record, "처리 실패: " + error.getMessage());
    }

    /**
     * 처리 실패 시 재시도 또는 DLQ 전송
     */
    private Mono<Void> handleProcessingFailure(ReceiverRecord<String, KafkaMatchingEvent> record, String reason) {
        int currentRetryCount = getCurrentRetryCount(record);

        log.warn("메시지 처리 실패 - OrderId: {}, 재시도: {}/{}, 사유: {}",
                record.value().getOrderId(), currentRetryCount, maxRetryCount, reason);

        if (currentRetryCount >= maxRetryCount) {
            // DLQ 전송
            return sendToDLQ(record, currentRetryCount, reason)
                    .then(acknowledgeRecord(record));
        } else {
            // 지연 토픽 전송
            return sendToDelayedTopic(record, currentRetryCount + 1, reason, "PROCESSING_FAILURE")
                    .then(acknowledgeRecord(record));
        }
    }

    /**
     * 서킷브레이커 실패 처리
     */
    private Mono<Void> handleCircuitBreakerFailure(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.defer(() -> {
            try {
                int currentRetryCount = getCurrentRetryCount(record);
                String reason = "서킷 브레이커 열림";

                if (currentRetryCount >= maxRetryCount) {
                    return sendToDLQ(record, currentRetryCount, reason)
                            .then(acknowledgeRecord(record));
                } else {
                    return sendToDelayedTopic(record, currentRetryCount + 1, reason, "CIRCUIT_BREAKER")
                            .then(acknowledgeRecord(record));
                }
            } catch (Exception e) {
                log.error("서킷 브레이커 처리 중 오류", e);
                return acknowledgeRecord(record);
            }
        });
    }

    /**
     * 지연 토픽으로 전송
     */
    private Mono<Void> sendToDelayedTopic(ReceiverRecord<String, KafkaMatchingEvent> record,
                                          int retryCount, String reason, String delayReason) {
        return Mono.fromRunnable(() -> {
            try {
                KafkaMatchingEvent originalEvent = record.value();
                String delayedTopic = "matching-delayed-retry";

                ProducerRecord<String, Object> producerRecord =
                        new ProducerRecord<>(delayedTopic, record.key(), originalEvent);

                // 헤더 설정
                producerRecord.headers().add(RETRY_COUNT_HEADER,
                        String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
                producerRecord.headers().add(ORIGINAL_TOPIC_HEADER,
                        getOriginalTopic(record).getBytes(StandardCharsets.UTF_8));
                producerRecord.headers().add(FAILURE_REASON_HEADER,
                        reason.getBytes(StandardCharsets.UTF_8));
                producerRecord.headers().add(DELAY_REASON_HEADER,
                        delayReason.getBytes(StandardCharsets.UTF_8));

                // 재시도 시간 계산
                Duration delay = calculateDelayByReason(delayReason, retryCount);
                long retryTimestamp = System.currentTimeMillis() + delay.toMillis();
                producerRecord.headers().add(RETRY_TIMESTAMP_HEADER,
                        String.valueOf(retryTimestamp).getBytes(StandardCharsets.UTF_8));

                kafkaTemplate.send(producerRecord);

                log.info("지연 토픽 전송 - OrderId: {}, 재시도: {}, 지연사유: {}, 대기시간: {}분",
                        originalEvent.getOrderId(), retryCount, delayReason, delay.toMinutes());

                // 메트릭 기록
                metricsCollector.recordDelayedRetry(delayReason, retryCount);

            } catch (Exception e) {
                log.error("지연 토픽 전송 실패", e);
            }
        });
    }

    /**
     * DLQ로 전송
     */
    private Mono<Void> sendToDLQ(ReceiverRecord<String, KafkaMatchingEvent> record,
                                 int finalRetryCount, String finalReason) {
        return Mono.fromRunnable(() -> {
            try {
                KafkaMatchingEvent originalEvent = record.value();

                DLQMessage dlqMessage = DLQMessage.builder()
                        .originalMessage(originalEvent)
                        .originalTopic(getOriginalTopic(record))
                        .originalPartition(record.partition())
                        .originalOffset(record.offset())
                        .originalKey(record.key())
                        .totalRetryCount(finalRetryCount)
                        .finalFailureReason(finalReason)
                        .dlqTimestamp(Instant.now())
                        .build();

                kafkaTemplate.send(dlqTopic, record.key(), dlqMessage);

                log.error("DLQ 전송 완료 - OrderId: {}, 최종 재시도: {}, 사유: {}",
                        originalEvent.getOrderId(), finalRetryCount, finalReason);

                // 메트릭 기록
                metricsCollector.recordDLQ(originalEvent.getOrderId().toString(), finalReason);

            } catch (Exception e) {
                log.error("DLQ 전송 중 오류", e);
            }
        });
    }

    /**
     * 현재 재시도 횟수 추출
     */
    private int getCurrentRetryCount(ReceiverRecord<String, KafkaMatchingEvent> record) {
        try {
            org.apache.kafka.common.header.Header retryHeader =
                    record.headers().lastHeader(RETRY_COUNT_HEADER);

            if (retryHeader != null) {
                String retryCountStr = new String(retryHeader.value(), StandardCharsets.UTF_8);
                return Integer.parseInt(retryCountStr);
            }
        } catch (Exception e) {
            log.warn("재시도 횟수 헤더 파싱 실패", e);
        }

        return 0;
    }

    /**
     * 원본 토픽 추출
     */
    private String getOriginalTopic(ReceiverRecord<String, KafkaMatchingEvent> record) {
        try {
            org.apache.kafka.common.header.Header originalTopicHeader =
                    record.headers().lastHeader(ORIGINAL_TOPIC_HEADER);

            if (originalTopicHeader != null) {
                return new String(originalTopicHeader.value(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("원본 토픽 헤더 파싱 실패", e);
        }

        return record.topic().contains("retry") ?
                record.topic().replace(".retry", "") : record.topic();
    }

    /**
     * 지연 사유별 대기 시간 계산
     */
    private Duration calculateDelayByReason(String delayReason, int retryCount) {
        return switch (delayReason) {
            case "CIRCUIT_BREAKER" -> {
                // Exponential backoff (1분, 2분, 4분, 최대 10분)
                long delayMinutes = Math.min((long) Math.pow(2, retryCount - 1), 10);
                yield Duration.ofMinutes(delayMinutes);
            }
            case "RECEIVE_SERVER_DOWN" -> {
                // 짧은 간격 (30초, 1분, 2분)
                long delaySeconds = Math.min(30 * retryCount, 120);
                yield Duration.ofSeconds(delaySeconds);
            }
            default -> {
                // 일반 처리 실패 (1분, 3분, 5분)
                long delayMinutes = Math.min(1 + (retryCount - 1) * 2, 5);
                yield Duration.ofMinutes(delayMinutes);
            }
        };
    }

    /**
     * 재시도 가능한 에러 판단
     */
    private boolean isRetryableError(Throwable throwable) {
        if (throwable instanceof ProcessingException ||
                throwable instanceof java.util.concurrent.TimeoutException ||
                throwable instanceof RedisCircuitBreakerException ||
                throwable instanceof RedisBackpressureException) {
            return true;
        }

        if (throwable.getCause() instanceof java.sql.SQLException) {
            return true;
        }

        String errorMessage = throwable.getMessage();
        if (errorMessage != null &&
                (errorMessage.contains("Redis") ||
                        errorMessage.contains("circuit breaker") ||
                        errorMessage.contains("백프레셔"))) {
            return true;
        }

        return false;
    }

    /**
     * 연속 에러 카운트 증가
     */
    private void incrementConsecutiveErrors(Throwable error) {
        int errorCount = consecutiveErrors.incrementAndGet();
        lastErrorTime.set(System.currentTimeMillis());

        if (errorCount >= MAX_CONSECUTIVE_ERRORS) {
            log.error("Circuit Breaker 임계값 도달 - {}초 후 재시도", CIRCUIT_BREAKER_TIMEOUT / 1000);
        }
    }

    /**
     * 서킷브레이커 상태 확인
     */
    private boolean isCircuitBreakerOpen() {
        if (consecutiveErrors.get() >= MAX_CONSECUTIVE_ERRORS) {
            long timeSinceLastError = System.currentTimeMillis() - lastErrorTime.get();

            if (timeSinceLastError > CIRCUIT_BREAKER_TIMEOUT) {
                log.info("Circuit Breaker Half-Open 상태로 전환");
                consecutiveErrors.set(0);
                return false;
            }

            return true;
        }

        return false;
    }

    /**
     * 컨슈머 재시작
     */
    private void restartConsumerAfterDelay() {
        Mono.delay(Duration.ofSeconds(5))
                .subscribe(tick -> {
                    if (consecutiveErrors.get() < MAX_CONSECUTIVE_ERRORS) {
                        startConsumer();
                    }
                });
    }

    /**
     * 토픽에서 버전 추출
     */
    private MatchingVersion extractVersionFromTopic(String topic) {
        String versionCode = topic.substring(topic.lastIndexOf('.') + 1);
        return MatchingVersion.fromCode(versionCode);
    }

    public static class ProcessingException extends RuntimeException {
        public ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}