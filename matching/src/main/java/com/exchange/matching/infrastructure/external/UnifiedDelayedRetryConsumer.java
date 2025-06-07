package com.exchange.matching.infrastructure.external;

import com.exchange.matching.infrastructure.dto.DLQMessage;
import com.exchange.matching.infrastructure.dto.KafkaMatchingEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class UnifiedDelayedRetryConsumer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ReceiveServerHealthMonitor healthMonitor;
    private final KafkaListenerEndpointRegistry registry;

    private static final String DELAYED_RETRY_TOPIC = "matching-delayed-retry";
    private static final String ORIGINAL_TOPIC = "user-to-matching.execute-order-delivery.v6d";
    private static final String DLQ_TOPIC = "matching-to-matching.execute-receiver-unavailable.dlq";

    /**
     * 통합 지연 재시도 토픽 컨슈머
     * Manual ACK로 안전한 처리 보장
     */
    @KafkaListener(
            topics = DELAYED_RETRY_TOPIC,
            containerFactory = "delayedRetryListenerContainerFactory", // Manual ACK 설정
            id = "delayed-retry-consumer"
    )
    public void consume(ConsumerRecord<String, KafkaMatchingEvent> record,
                        Acknowledgment ack) {
        try {
            String delayReason = getDelayReason(record);
            log.debug("지연 메시지 처리 시작: OrderId={}, 지연사유={}",
                    record.value().getOrderId(), delayReason);

            // 1. 재시도 시간 체크
            if (!isRetryTimeReached(record)) {
                log.debug("재시도 시간 미도달 - 처리 보류: OrderId={}",
                        record.value().getOrderId());
                // ACK 하지 않음 - 메시지 보존
                return;
            }

            // 2. 서버 상태 체크 (지연 사유별로)
            if (!canProcessMessage(record)) {
                log.debug("처리 조건 미충족 - 처리 보류: OrderId={}, 사유={}",
                        record.value().getOrderId(), delayReason);
                // ACK 하지 않음 - 서버 복구까지 대기
                return;
            }

            // 3. 재시도 횟수 체크
            int retryCount = getRetryCount(record);
            if (retryCount >= 3) {
                log.error("최대 재시도 횟수 초과 - DLQ 전송: OrderId={}, 재시도={}",
                        record.value().getOrderId(), retryCount);
                sendToDLQ(record, retryCount);
                ack.acknowledge(); // ✅ DLQ 전송 후 ACK
                return;
            }

            // 4. 원본 토픽으로 재전송
            log.info("지연 메시지를 원본 토픽으로 재전송: OrderId={}, 재시도={}",
                    record.value().getOrderId(), retryCount);

            kafkaTemplate.send(ORIGINAL_TOPIC, record.key(), record.value());
            ack.acknowledge(); // ✅ 성공 후 ACK

        } catch (Exception e) {
            log.error("지연 재시도 처리 실패: OrderId={} - ACK 안함",
                    record.value().getOrderId(), e);
            // 예외 발생시 ACK 안함 - 메시지 보존
        }
    }

    /**
     * 메시지 처리 가능 여부 판단 (지연 사유별)
     */
    private boolean canProcessMessage(ConsumerRecord<String, KafkaMatchingEvent> record) {
        String delayReason = getDelayReason(record);

        switch (delayReason) {
            case "RECEIVE_SERVER_DOWN":
                return healthMonitor.isHealthy();

            case "CIRCUIT_BREAKER":
                // 서킷브레이커는 시간 기반으로만 판단 (isRetryTimeReached에서 처리)
                return true;

            default:
                return healthMonitor.isHealthy();
        }
    }

    /**
     * 재시도 시간 도달 여부 확인
     */
    private boolean isRetryTimeReached(ConsumerRecord<String, KafkaMatchingEvent> record) {
        try {
            org.apache.kafka.common.header.Header timestampHeader =
                    record.headers().lastHeader("X-Retry-Timestamp");

            if (timestampHeader == null) {
                return true; // 타임스탬프 없으면 즉시 처리
            }

            long retryTimestamp = Long.parseLong(
                    new String(timestampHeader.value(), StandardCharsets.UTF_8));
            long currentTime = System.currentTimeMillis();

            boolean timeReached = currentTime >= retryTimestamp;

            if (!timeReached) {
                long remainingMinutes = (retryTimestamp - currentTime) / 60000;
                log.debug("재시도 대기: OrderId={}, {}분 후 처리 가능",
                        record.value().getOrderId(), remainingMinutes);
            }

            return timeReached;

        } catch (Exception e) {
            log.warn("재시도 시간 파싱 실패 - 즉시 처리: OrderId={}",
                    record.value().getOrderId(), e);
            return true;
        }
    }

    /**
     * 지연 사유 추출
     */
    private String getDelayReason(ConsumerRecord<String, KafkaMatchingEvent> record) {
        try {
            org.apache.kafka.common.header.Header reasonHeader =
                    record.headers().lastHeader("X-Delay-Reason");

            if (reasonHeader != null) {
                return new String(reasonHeader.value(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.warn("지연 사유 파싱 실패", e);
        }

        return "UNKNOWN";
    }

    /**
     * 재시도 횟수 추출
     */
    private int getRetryCount(ConsumerRecord<String, KafkaMatchingEvent> record) {
        try {
            org.apache.kafka.common.header.Header retryHeader =
                    record.headers().lastHeader("X-Retry-Count");

            if (retryHeader != null) {
                return Integer.parseInt(
                        new String(retryHeader.value(), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.warn("재시도 횟수 파싱 실패", e);
        }

        return 0;
    }

    /**
     * DLQ 전송
     */
    private void sendToDLQ(ConsumerRecord<String, KafkaMatchingEvent> record, int finalRetryCount) {
        try {
            DLQMessage dlqMessage = DLQMessage.builder()
                    .originalMessage(record.value())
                    .originalTopic(ORIGINAL_TOPIC)
                    .originalPartition(record.partition())
                    .originalOffset(record.offset())
                    .originalKey(record.key())
                    .totalRetryCount(finalRetryCount)
                    .finalFailureReason("지연 재시도 최대 횟수 초과")
                    .dlqTimestamp(Instant.now())
                    .build();

            kafkaTemplate.send(DLQ_TOPIC, record.key(), dlqMessage);

            log.error("DLQ 전송 완료 - OrderId: {}, 최종 재시도: {}",
                    record.value().getOrderId(), finalRetryCount);

        } catch (Exception e) {
            log.error("DLQ 전송 실패", e);
            throw e;
        }
    }

    /**
     * 서버 상태 변화 이벤트 처리
     */
    @EventListener
    public void handleHealthStatusChange(HealthStatusChangeEvent event) {
        if (event.isHealthy()) {
            log.info("서버 복구 - 지연 재시도 컨슈머 재개");
            resumeConsumer();
        } else {
            log.warn("서버 다운 - 지연 재시도 컨슈머 일시정지");
            pauseConsumer();
        }
    }

    private void pauseConsumer() {
        try {
            registry.getListenerContainer("delayed-retry-consumer").pause();
        } catch (Exception e) {
            log.error("컨슈머 일시정지 실패", e);
        }
    }

    private void resumeConsumer() {
        try {
            registry.getListenerContainer("delayed-retry-consumer").resume();
        } catch (Exception e) {
            log.error("컨슈머 재개 실패", e);
        }
    }
}