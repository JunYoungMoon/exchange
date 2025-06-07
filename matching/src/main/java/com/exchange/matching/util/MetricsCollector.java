package com.exchange.matching.util;

import com.exchange.matching.application.enums.MatchingVersion;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Slf4j
@Component
public class MetricsCollector {
    private final MeterRegistry meterRegistry;

    // 기본 카운터들
    private final Counter processedCounter;
    private final Counter errorCounter;
    private final Counter dlqCounter;

    // 재시도 관련 카운터들
    private final Map<String, Counter> delayedRetryCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> dlqReasonCounters = new ConcurrentHashMap<>();

    // 타이머들
    private final Map<MatchingVersion, Timer> versionTimers = new ConcurrentHashMap<>();
    private final Map<String, Timer> retryDelayTimers = new ConcurrentHashMap<>();

    public MetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        // 기본 메트릭 초기화
        this.processedCounter = Counter.builder("matching_events_processed_total")
                .description("Total number of matching events processed successfully")
                .register(meterRegistry);

        this.errorCounter = Counter.builder("matching_events_error_total")
                .description("Total number of matching events processing errors")
                .register(meterRegistry);

        this.dlqCounter = Counter.builder("matching_events_dlq_total")
                .description("Total number of events sent to DLQ")
                .register(meterRegistry);
    }

    /**
     * 동기 처리 시간 기록
     */
    public void recordProcessing(Runnable task, MatchingVersion version) {
        Timer timer = getVersionTimer(version);

        timer.record(() -> {
            try {
                task.run();
                processedCounter.increment();
                log.debug("처리 완료 메트릭 기록: version={}", version.getCode());
            } catch (Exception e) {
                errorCounter.increment();
                log.warn("처리 중 에러 발생: version={}, error={}", version.getCode(), e.getMessage());
                throw e;
            }
        });
    }

    /**
     * 리액티브 처리 시간 기록
     */
    public <T> Mono<T> recordProcessingReactive(Supplier<Mono<T>> taskSupplier, MatchingVersion version) {
        Timer timer = getVersionTimer(version);
        long startTime = System.nanoTime();

        return taskSupplier.get()
                .doOnSuccess(result -> {
                    timer.record(Duration.ofNanos(System.nanoTime() - startTime));
                    processedCounter.increment();
                    log.debug("리액티브 처리 완료 메트릭 기록: version={}", version.getCode());
                })
                .doOnError(error -> {
                    timer.record(Duration.ofNanos(System.nanoTime() - startTime));
                    errorCounter.increment();
                    log.warn("리액티브 처리 중 에러: version={}, error={}",
                            version.getCode(), error.getMessage());
                });
    }

    /**
     * 지연 재시도 메트릭 기록
     */
    public void recordDelayedRetry(String delayReason, int retryCount) {
        try {
            // 지연 사유별 카운터
            Counter delayCounter = delayedRetryCounters.computeIfAbsent(delayReason, reason ->
                    Counter.builder("matching_delayed_retry_total")
                            .description("Total number of delayed retry attempts")
                            .tag("delay_reason", reason)
                            .register(meterRegistry)
            );
            delayCounter.increment();

            // 재시도 횟수별 타이머 (지연 시간 측정용)
            String timerKey = delayReason + "_retry_" + retryCount;
            Timer retryTimer = retryDelayTimers.computeIfAbsent(timerKey, key ->
                    Timer.builder("matching_retry_delay_duration")
                            .description("Time between retry attempts")
                            .tag("delay_reason", delayReason)
                            .tag("retry_count", String.valueOf(retryCount))
                            .register(meterRegistry)
            );

            log.info("지연 재시도 메트릭 기록: delayReason={}, retryCount={}", delayReason, retryCount);

        } catch (Exception e) {
            log.error("지연 재시도 메트릭 기록 실패", e);
        }
    }

    /**
     * DLQ 전송 메트릭 기록
     */
    public void recordDLQ(String orderId, String failureReason) {
        try {
            // 전체 DLQ 카운터
            dlqCounter.increment();

            // 실패 사유별 DLQ 카운터
            Counter reasonCounter = dlqReasonCounters.computeIfAbsent(failureReason, reason ->
                    Counter.builder("matching_dlq_by_reason_total")
                            .description("Total number of DLQ events by failure reason")
                            .tag("failure_reason", sanitizeTag(reason))
                            .register(meterRegistry)
            );
            reasonCounter.increment();

            log.warn("DLQ 메트릭 기록: orderId={}, reason={}", orderId, failureReason);

        } catch (Exception e) {
            log.error("DLQ 메트릭 기록 실패: orderId={}", orderId, e);
        }
    }

    /**
     * 버전별 타이머 조회/생성
     */
    private Timer getVersionTimer(MatchingVersion version) {
        return versionTimers.computeIfAbsent(version, v ->
                Timer.builder("matching_processing_duration")
                        .description("Time taken to process matching events")
                        .tag("version", v.getCode())
                        .register(meterRegistry)
        );
    }

    /**
     * 메트릭 태그용 문자열 정제 (특수문자 제거)
     */
    private String sanitizeTag(String input) {
        if (input == null) {
            return "unknown";
        }

        return input.replaceAll("[^a-zA-Z0-9_-]", "_")
                .toLowerCase()
                .substring(0, Math.min(input.length(), 50)); // 길이 제한
    }
}