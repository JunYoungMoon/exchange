package com.exchange.matching.infrastructure.external;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.service.MatchingApplicationService;
import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import com.exchange.matching.util.MetricsCollector;

@Slf4j
//@Component
@RequiredArgsConstructor
public class EventConsumer {
    private final MatchingApplicationService matchingService;
    private final MetricsCollector metricsCollector;
    private final ReceiveServerHealthMonitor healthMonitor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = {
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
            },
            containerFactory = "orderDeliveryKafkaListenerContainerFactory",
            concurrency = "10"
    )
    public void consume(ConsumerRecord<String, KafkaMatchingEvent> record) {
        // 캐싱된 상태를 즉시 확인 (네트워크 호출 없음)
//        if (!healthMonitor.isHealthy()) {
//            log.info("Receive 서버 상태 이상 감지: 처리 불가 메시지를 재시도 큐로 전송합니다.");
//            kafkaTemplate.send("matching-to-matching.execute-receiver-unavailable.retry", record.key(), record.value());
//            return;
//        }

        String topic = record.topic();
        MatchingVersion version = extractVersionFromTopic(topic);

        metricsCollector.recordProcessing(() -> {
            KafkaMatchingEvent event = record.value();
            CreateMatchingCommand command = KafkaMatchingEvent.commandFromEvent(event);
            matchingService.processMatching(command, version);
        }, version);
    }

    private MatchingVersion extractVersionFromTopic(String topic) {
        String versionCode = topic.substring(topic.lastIndexOf('.') + 1);
        return MatchingVersion.fromCode(versionCode);
    }
}