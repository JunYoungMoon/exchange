package com.exchange.matching.infrastructure.external;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.service.MatchingApplicationService;
import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.matching.util.MetricsCollector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveEventConsumer {
    private final MatchingApplicationService matchingService;
    private final MetricsCollector metricsCollector;
    private final KafkaReceiver<String, KafkaMatchingEvent> kafkaReceiver;

    private final AtomicBoolean running = new AtomicBoolean(true);

    @EventListener(ApplicationStartedEvent.class)
    public void startConsumer() {
        kafkaReceiver.receive()
                .publishOn(Schedulers.boundedElastic())
                .filter(record -> {
                    if (!running.get()) {
                        record.receiverOffset().acknowledge();
                        return false;
                    }
                    return true;
                })
                .flatMap(this::processRecord)
                .subscribe(
                        success -> {},
                        error -> log.error("Kafka 스트림 처리 오류", error),
                        () -> log.info("Kafka 스트림 처리 완료")
                );
    }

    private Mono<Void> processRecord(ReceiverRecord<String, KafkaMatchingEvent> record) {
        return Mono.defer(() -> {
            String topic = record.topic();
            MatchingVersion version = extractVersionFromTopic(topic);
            KafkaMatchingEvent event = record.value();
            CreateMatchingCommand command = KafkaMatchingEvent.commandFromEvent(event);

            return metricsCollector.recordProcessingReactive(() ->
                            matchingService.processMatchingReactive(command, version), version)
                    .then(Mono.fromRunnable(() -> record.receiverOffset().acknowledge()))
                    .then();

        });
    }

    private MatchingVersion extractVersionFromTopic(String topic) {
        String versionCode = topic.substring(topic.lastIndexOf('.') + 1);
        return MatchingVersion.fromCode(versionCode);
    }
}