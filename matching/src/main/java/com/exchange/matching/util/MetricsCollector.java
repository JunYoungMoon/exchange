package com.exchange.matching.util;

import com.exchange.matching.application.enums.MatchingVersion;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Component
public class MetricsCollector {
    private final MeterRegistry meterRegistry;
    private final Counter processedCounter;
    private final Map<MatchingVersion, Timer> versionTimers = new ConcurrentHashMap<>();

    public MetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.processedCounter = Counter.builder("matching_events_processed_total")
                .description("Total number of matching events processed")
                .register(meterRegistry);
    }

    public void recordProcessing(Runnable task, MatchingVersion version) {
        Timer timer = versionTimers.computeIfAbsent(version, v ->
                Timer.builder("matching_processing_time")
                        .description("Time taken to process matching events")
                        .tag("version", v.getCode())
                        .register(meterRegistry)
        );

        timer.record(() -> {
            task.run();
            processedCounter.increment();
        });
    }

    public <T> Mono<T> recordProcessingReactive(Supplier<Mono<T>> taskSupplier, MatchingVersion version) {
        Timer timer = versionTimers.computeIfAbsent(version, v ->
                Timer.builder("matching_processing_time")
                        .description("Time taken to process matching events")
                        .tag("version", v.getCode())
                        .tag("mode", "reactive")  // 리액티브 모드 태그 추가
                        .register(meterRegistry)
        );

        long startTime = System.nanoTime();

        return taskSupplier.get()
                .doOnSuccess(result -> {
                    timer.record(Duration.ofNanos(System.nanoTime() - startTime));
                    processedCounter.increment();
                })
                .doOnError(error -> {
                    // 에러가 발생해도 처리 시간은 기록
                    timer.record(Duration.ofNanos(System.nanoTime() - startTime));
                    // 필요하다면 여기에 에러 카운터 추가 가능
                });
    }
}