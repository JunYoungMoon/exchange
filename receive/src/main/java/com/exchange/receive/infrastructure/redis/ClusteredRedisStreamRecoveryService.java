package com.exchange.receive.infrastructure.redis;

import com.exchange.receive.infrastructure.cassandra.ShardCalculator;
import com.exchange.receive.infrastructure.dto.KafkaMatchedOrderEvent;
import com.exchange.receive.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.receive.infrastructure.dto.KafkaOrderStoreEvent;
import com.exchange.receive.infrastructure.enums.OperationType;
import com.exchange.receive.infrastructure.enums.OrderType;
import com.exchange.receive.infrastructure.enums.TradingPair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class ClusteredRedisStreamRecoveryService {

    private final ExecutorService kafkaExecutorService = Executors.newFixedThreadPool(20);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

    // Kafka 토픽
    private static final String MATCH_KAFKA_TOPIC = "matching-to-order_completed.execute-order-matched";
    private static final String UNMATCH_KAFKA_TOPIC = "matching-to-order_completed.execute-order-unmatched";
    private static final String PARTIAL_MATCHED_KAFKA_TOPIC = "user-to-matching.execute-order-delivery.v6d";

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ShardCalculator shardCalculator;

    private String consumerGroupName;
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private volatile boolean shuttingDown = false;

    /**
     * 서비스 초기화
     */
    public void initialize(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;

        // 각 거래쌍별로 독립적인 Pending 메시지 처리 스케줄러 시작
        for (TradingPair tradingPair : TradingPair.values()) {
            scheduledExecutorService.scheduleWithFixedDelay(
                    () -> processPendingMessagesForTradingPair(tradingPair),
                    30, // 초기 지연
                    30, // 실행 간격 (30초)
                    TimeUnit.SECONDS
            );
        }

        log.info("클러스터 Redis Stream Recovery Service 초기화 완료 - Group: {}", consumerGroupName);
    }

    /**
     * 서비스 종료
     */
    public void shutdown() {
        log.info("클러스터 Redis Stream Recovery Service 종료 중...");
        shuttingDown = true;

        // 스케줄러 종료
        scheduledExecutorService.shutdown();

        try {
            if (!scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduledExecutorService.shutdownNow();
        }

        // 모든 pending 메시지가 처리될 때까지 대기 (최대 30초)
        int maxWaitSeconds = 30;
        for (int i = 0; i < maxWaitSeconds; i++) {
            int count = pendingCount.get();
            if (count <= 0) {
                break;
            }
            log.info("Pending 메시지 처리 대기 중: {}", count);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Executor service 종료
        kafkaExecutorService.shutdown();
        log.info("클러스터 Redis Stream Recovery Service 종료 완료");
    }

    /**
     * 특정 거래쌍의 모든 스트림에 대한 Pending 메시지 처리
     */
    private void processPendingMessagesForTradingPair(TradingPair tradingPair) {
        if (shuttingDown) {
            return;
        }

        try {
            log.debug("거래쌍 {} Pending 메시지 처리 시작", tradingPair.getSymbol());

            // 각 스트림의 Pending 메시지 처리
            processPendingMessagesForStream(tradingPair.getMatchStreamKey(), tradingPair, StreamType.MATCH);
            processPendingMessagesForStream(tradingPair.getUnmatchStreamKey(), tradingPair, StreamType.UNMATCH);
            processPendingMessagesForStream(tradingPair.getPartialMatchedStreamKey(), tradingPair, StreamType.PARTIAL_MATCH);

        } catch (Exception e) {
            log.error("거래쌍 {} Pending 메시지 처리 중 오류", tradingPair.getSymbol(), e);
        }
    }

    /**
     * 특정 스트림의 Pending 메시지 처리
     */
    private void processPendingMessagesForStream(String streamKey, TradingPair tradingPair, StreamType streamType) {
        try {
            // 처리되지 않은 메시지 조회 (10분 이상 경과된 메시지)
            PendingMessages pendingMessages = redisTemplate.opsForStream()
                    .pending(streamKey, consumerGroupName, Range.unbounded(), 100);

            if (pendingMessages.isEmpty()) {
                return;
            }

            log.info("거래쌍 {} {} 스트림에서 처리되지 않은 메시지 {} 개 발견",
                    tradingPair.getSymbol(), streamType, pendingMessages.size());

            for (PendingMessage pending : pendingMessages) {
                // 10분(600,000ms) 이상 지난 메시지만 재처리
                if (pending.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofMinutes(10)) > 0) {
                    String messageId = pending.getIdAsString();
                    log.info("거래쌍 {} Pending 메시지 재처리: {} (지연: {}ms)",
                            tradingPair.getSymbol(), messageId, pending.getElapsedTimeSinceLastDelivery().toMillis());

                    // 메시지 다시 가져오기
                    List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                            .range(streamKey, Range.closed(messageId, messageId));

                    if (records != null && !records.isEmpty()) {
                        MapRecord<String, Object, Object> record = records.get(0);

                        // 메시지 다시 처리
                        pendingCount.incrementAndGet();
                        try {
                            switch (streamType) {
                                case MATCH:
                                    processRawMatchMessage(record, tradingPair);
                                    break;
                                case UNMATCH:
                                    processRawUnmatchMessage(record, tradingPair);
                                    break;
                                case PARTIAL_MATCH:
                                    processRawPartialMatchMessage(record, tradingPair);
                                    break;
                            }
                        } finally {
                            pendingCount.decrementAndGet();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("거래쌍 {} {} 스트림의 Pending 메시지 처리 중 오류",
                    tradingPair.getSymbol(), streamType, e);
        }
    }

    /**
     * 스트림 타입 열거형
     */
    private enum StreamType {
        MATCH, UNMATCH, PARTIAL_MATCH
    }

    /**
     * 매칭 메시지 재처리
     */
    private void processRawMatchMessage(MapRecord<String, Object, Object> record, TradingPair tradingPair) {
        try {
            String messageId = record.getId().getValue();
            Map<String, String> body = convertMapEntriesToMap(record.getValue());
            String streamKey = tradingPair.getMatchStreamKey();

            Instant instant = Instant.ofEpochSecond(Long.parseLong(body.get("timestamp")));
            LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();

            KafkaMatchedOrderEvent matchedEvent = KafkaMatchedOrderEvent.builder()
                    .tradingPair(body.get("tradingPair"))
                    .executionPrice(new BigDecimal(body.get("executionPrice")))
                    .matchedQuantity(new BigDecimal(body.get("matchedQuantity")))
                    .buyUserId(UUID.fromString(body.get("buyUserId")))
                    .sellUserId(UUID.fromString(body.get("sellUserId")))
                    .buyMatchedOrderId(UUID.randomUUID())
                    .sellMatchedOrderId(UUID.randomUUID())
                    .createdAt(instant)
                    .yearMonthDate(localDate)
                    .buyShard(shardCalculator.calculateShard(UUID.fromString(body.get("buyOrderId"))))
                    .sellShard(shardCalculator.calculateShard(UUID.fromString(body.get("sellOrderId"))))
                    .build();

            CompletableFuture<SendResult<String, Object>> future =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            return kafkaTemplate.send(MATCH_KAFKA_TOPIC, matchedEvent).get();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, kafkaExecutorService);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    redisTemplate.opsForStream().acknowledge(streamKey, consumerGroupName, messageId);
                    log.info("거래쌍 {} 복구 매칭 메시지 Kafka 전송 완료: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("거래쌍 {} 복구 매칭 메시지 Kafka 전송 실패: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("거래쌍 {} 복구 매칭 메시지 처리 오류", tradingPair.getSymbol(), e);
        }
    }

    /**
     * 미체결 메시지 재처리
     */
    private void processRawUnmatchMessage(MapRecord<String, Object, Object> record, TradingPair tradingPair) {
        try {
            String messageId = record.getId().getValue();
            Map<String, String> body = convertMapEntriesToMap(record.getValue());
            String streamKey = tradingPair.getUnmatchStreamKey();

            long timestamp = Long.parseLong(body.get("timestamp"));
            long processedTimestamp = "BUY".equals(body.get("orderType"))
                    ? 9999999999999L - timestamp
                    : timestamp;

            Instant instant = Instant.ofEpochMilli(processedTimestamp);
            LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();

            KafkaOrderStoreEvent event = KafkaOrderStoreEvent.builder()
                    .tradingPair(body.get("tradingPair"))
                    .orderType(OrderType.valueOf(body.get("orderType")))
                    .price(new BigDecimal(body.get("price")))
                    .quantity(new BigDecimal(body.get("quantity")))
                    .userId(UUID.fromString(body.get("userId")))
                    .orderId(UUID.fromString(body.get("orderId")))
                    .startTime(Long.parseLong(body.get("timestamp")))
                    .operationType(OperationType.valueOf(body.get("operation")))
                    .shard(shardCalculator.calculateShard(UUID.fromString(body.get("orderId"))))
                    .yearMonthDate(localDate)
                    .createdAt(instant)
                    .build();

            CompletableFuture<SendResult<String, Object>> future =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            return kafkaTemplate.send(UNMATCH_KAFKA_TOPIC, body.get("orderId"), event).get();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, kafkaExecutorService);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    redisTemplate.opsForStream().acknowledge(streamKey, consumerGroupName, messageId);
                    log.info("거래쌍 {} 복구 미체결 메시지 Kafka 전송 완료: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("거래쌍 {} 복구 미체결 메시지 Kafka 전송 실패: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("거래쌍 {} 복구 미체결 메시지 처리 오류", tradingPair.getSymbol(), e);
        }
    }

    /**
     * 부분체결 메시지 재처리
     */
    private void processRawPartialMatchMessage(MapRecord<String, Object, Object> record, TradingPair tradingPair) {
        try {
            String messageId = record.getId().getValue();
            Map<String, String> body = convertMapEntriesToMap(record.getValue());
            String streamKey = tradingPair.getPartialMatchedStreamKey();

            KafkaMatchingEvent event = KafkaMatchingEvent.builder()
                    .tradingPair(body.get("tradingPair"))
                    .orderType(OrderType.valueOf(body.get("orderType")))
                    .price(new BigDecimal(body.get("price")))
                    .quantity(new BigDecimal(body.get("quantity")))
                    .userId(UUID.fromString(body.get("userId")))
                    .orderId(UUID.fromString(body.get("orderId")))
                    .build();

            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(PARTIAL_MATCHED_KAFKA_TOPIC, body.get("orderId"), event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    redisTemplate.opsForStream().acknowledge(streamKey, consumerGroupName, messageId);
                    log.info("거래쌍 {} 복구 부분체결 메시지 Kafka 전송 완료: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("거래쌍 {} 복구 부분체결 메시지 Kafka 전송 실패: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("거래쌍 {} 복구 부분체결 메시지 처리 오류", tradingPair.getSymbol(), e);
        }
    }

    /**
     * Map<Object, Object>를 Map<String, String>으로 변환
     */
    private Map<String, String> convertMapEntriesToMap(Map<Object, Object> entries) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return result;
    }
}