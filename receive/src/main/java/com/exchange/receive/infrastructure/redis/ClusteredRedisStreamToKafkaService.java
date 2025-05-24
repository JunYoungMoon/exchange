package com.exchange.receive.infrastructure.redis;

import com.exchange.receive.infrastructure.cassandra.ShardCalculator;
import com.exchange.receive.infrastructure.dto.KafkaMatchedOrderEvent;
import com.exchange.receive.infrastructure.dto.KafkaMatchingEvent;
import com.exchange.receive.infrastructure.dto.KafkaOrderStoreEvent;
import com.exchange.receive.infrastructure.enums.OperationType;
import com.exchange.receive.infrastructure.enums.OrderType;
import com.exchange.receive.infrastructure.enums.TradingPair;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class ClusteredRedisStreamToKafkaService {

    private final ExecutorService kafkaExecutorService = Executors.newFixedThreadPool(20);

    // Kafka 토픽
    private static final String MATCH_KAFKA_TOPIC = "matching-to-order_completed.execute-order-matched";
    private static final String UNMATCH_KAFKA_TOPIC = "matching-to-order_completed.execute-order-unmatched";
    private static final String PARTIAL_MATCHED_KAFKA_TOPIC = "user-to-matching.execute-order-delivery.v6d";

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ClusteredRedisStreamRecoveryService recoveryService;
    private final ShardCalculator shardCalculator;

    // 거래쌍별 리스너 컨테이너와 구독
    private final Map<TradingPair, StreamMessageListenerContainer<String, MapRecord<String, String, String>>> listenerContainers = new HashMap<>();
    private final Map<TradingPair, List<Subscription>> subscriptions = new HashMap<>();

    private String consumerGroupName;
    private AtomicInteger pendingCount = new AtomicInteger(0);

    @PostConstruct
    public void init() throws UnknownHostException {
        // Consumer 그룹 및 이름 설정
        String hostname = InetAddress.getLocalHost().getHostName();
        String consumerName = hostname + "-" + System.currentTimeMillis();
        this.consumerGroupName = "matching-service-group";

        // 각 거래쌍에 대해 별도의 리스너 컨테이너 생성
        for (TradingPair tradingPair : TradingPair.values()) {
            initTradingPairStreams(tradingPair, consumerName);
        }

        // 복구 서비스 초기화
        recoveryService.initialize(consumerGroupName);

        log.info("클러스터 Redis Stream Consumer 시작 완료 - Group: {}, Consumer: {}", consumerGroupName, consumerName);
    }

    private void initTradingPairStreams(TradingPair tradingPair, String consumerName) {
        log.info("거래쌍 {} 스트림 초기화 시작", tradingPair.getSymbol());

        // 스트림 키들
        String matchStreamKey = tradingPair.getMatchStreamKey();
        String unmatchStreamKey = tradingPair.getUnmatchStreamKey();
        String partialMatchedStreamKey = tradingPair.getPartialMatchedStreamKey();
        String coldDataRequestStreamKey = tradingPair.getColdDataRequestStreamKey();

        // Consumer 그룹 생성
        createConsumerGroupIfNotExists(matchStreamKey, consumerGroupName);
        createConsumerGroupIfNotExists(unmatchStreamKey, consumerGroupName);
        createConsumerGroupIfNotExists(partialMatchedStreamKey, consumerGroupName);
        createConsumerGroupIfNotExists(coldDataRequestStreamKey, consumerGroupName);

        // 리스너 컨테이너 설정
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .pollTimeout(Duration.ofMillis(100))
                        .batchSize(50)
                        .executor(Executors.newFixedThreadPool(5)) // 거래쌍별로 스레드 풀 분리
                        .errorHandler((e) -> log.warn("Redis Streams Listener 오류 - {}: {}", tradingPair.getSymbol(), e.getMessage()))
                        .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                StreamMessageListenerContainer.create(
                        Objects.requireNonNull(redisTemplate.getConnectionFactory()),
                        options
                );

        // Consumer 생성
        Consumer consumer = Consumer.from(consumerGroupName, consumerName + "-" + tradingPair.name());

        // 구독 리스트
        List<Subscription> tradingPairSubscriptions = new ArrayList<>();

        // 각 스트림 구독
        tradingPairSubscriptions.add(container.receive(
                consumer,
                StreamOffset.create(matchStreamKey, ReadOffset.lastConsumed()),
                new MatchStreamListener(tradingPair)
        ));

        tradingPairSubscriptions.add(container.receive(
                consumer,
                StreamOffset.create(unmatchStreamKey, ReadOffset.lastConsumed()),
                new UnmatchStreamListener(tradingPair)
        ));

        tradingPairSubscriptions.add(container.receive(
                consumer,
                StreamOffset.create(partialMatchedStreamKey, ReadOffset.lastConsumed()),
                new PartialStreamListener(tradingPair)
        ));

        tradingPairSubscriptions.add(container.receive(
                consumer,
                StreamOffset.create(coldDataRequestStreamKey, ReadOffset.lastConsumed()),
                new ColdDataRequestListener(tradingPair)
        ));

        // 컨테이너 시작
        container.start();

        // 저장
        listenerContainers.put(tradingPair, container);
        subscriptions.put(tradingPair, tradingPairSubscriptions);

        log.info("거래쌍 {} 스트림 초기화 완료", tradingPair.getSymbol());
    }

    @PreDestroy
    public void shutdown() {
        log.info("클러스터 Redis Stream Consumer 종료 중...");

        // 복구 서비스 종료
        recoveryService.shutdown();

        // 모든 거래쌍의 구독 취소
        for (Map.Entry<TradingPair, List<Subscription>> entry : subscriptions.entrySet()) {
            TradingPair tradingPair = entry.getKey();
            List<Subscription> subs = entry.getValue();

            log.info("거래쌍 {} 구독 취소 중...", tradingPair.getSymbol());
            for (Subscription sub : subs) {
                if (sub != null) {
                    sub.cancel();
                }
            }
        }

        // 모든 리스너 컨테이너 종료
        for (Map.Entry<TradingPair, StreamMessageListenerContainer<String, MapRecord<String, String, String>>> entry : listenerContainers.entrySet()) {
            TradingPair tradingPair = entry.getKey();
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = entry.getValue();

            log.info("거래쌍 {} 컨테이너 종료 중...", tradingPair.getSymbol());
            if (container != null) {
                container.stop();
            }
        }

        kafkaExecutorService.shutdown();
        log.info("클러스터 Redis Stream Consumer 종료 완료");
    }

    private void createConsumerGroupIfNotExists(String streamKey, String groupName) {
        try {
            // 스트림이 존재하지 않으면 생성
            Boolean streamExists = redisTemplate.hasKey(streamKey);
            if (!streamExists) {
                Map<String, String> initialData = new HashMap<>();
                initialData.put("init", "true");
                redisTemplate.opsForStream().add(streamKey, initialData);
                log.info("스트림 생성: {}", streamKey);
            }

            // Consumer 그룹 생성 시도
            redisTemplate.opsForStream().createGroup(streamKey, groupName);
            log.info("Consumer 그룹 생성: {} - {}", streamKey, groupName);

        } catch (Exception e) {
            // BUSYGROUP 오류 처리
            Throwable cause = e;
            boolean isBusyGroupError = false;

            while (cause != null) {
                if (cause instanceof io.lettuce.core.RedisBusyException ||
                        (cause.getMessage() != null && cause.getMessage().contains("BUSYGROUP"))) {
                    isBusyGroupError = true;
                    break;
                }
                cause = cause.getCause();
            }

            if (isBusyGroupError) {
                log.debug("Consumer 그룹 이미 존재: {} - {}", streamKey, groupName);
            } else {
                log.warn("Consumer 그룹 생성 오류: {} - {}", streamKey, groupName, e);
            }
        }
    }

    /**
     * 매칭 스트림 리스너
     */
    private class MatchStreamListener implements StreamListener<String, MapRecord<String, String, String>> {
        private final TradingPair tradingPair;

        public MatchStreamListener(TradingPair tradingPair) {
            this.tradingPair = tradingPair;
        }

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            pendingCount.incrementAndGet();
            try {
                processMatchMessage(message, tradingPair);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    /**
     * 미체결 스트림 리스너
     */
    private class UnmatchStreamListener implements StreamListener<String, MapRecord<String, String, String>> {
        private final TradingPair tradingPair;

        public UnmatchStreamListener(TradingPair tradingPair) {
            this.tradingPair = tradingPair;
        }

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            pendingCount.incrementAndGet();
            try {
                processUnmatchMessage(message, tradingPair);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    /**
     * 부분 체결 스트림 리스너
     */
    private class PartialStreamListener implements StreamListener<String, MapRecord<String, String, String>> {
        private final TradingPair tradingPair;

        public PartialStreamListener(TradingPair tradingPair) {
            this.tradingPair = tradingPair;
        }

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            pendingCount.incrementAndGet();
            try {
                processPartialMatchMessage(message, tradingPair);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    /**
     * 콜드 데이터 요청 리스너
     */
    private class ColdDataRequestListener implements StreamListener<String, MapRecord<String, String, String>> {
        private final TradingPair tradingPair;

        public ColdDataRequestListener(TradingPair tradingPair) {
            this.tradingPair = tradingPair;
        }

        @Override
        public void onMessage(MapRecord<String, String, String> message) {
            try {
                processColdDataRequest(message, tradingPair);
            } catch (Exception e) {
                log.error("콜드 데이터 요청 처리 오류 - {}: {}", tradingPair.getSymbol(), e.getMessage(), e);
            }
        }
    }

    /**
     * 매칭 메시지 처리 및 Kafka로 전송
     */
    private void processMatchMessage(MapRecord<String, String, String> message, TradingPair tradingPair) {
        try {
            String messageId = message.getId().getValue();
            Map<String, String> body = message.getValue();
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
                    log.debug("매칭 이벤트 Kafka 전송 완료 - {}: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("매칭 이벤트 Kafka 전송 실패 - {}: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("매칭 메시지 처리 오류 - {}: {}", tradingPair.getSymbol(), e.getMessage(), e);
        }
    }

    /**
     * 미체결 메시지 처리 및 Kafka로 전송
     */
    private void processUnmatchMessage(MapRecord<String, String, String> message, TradingPair tradingPair) {
        try {
            String messageId = message.getId().getValue();
            Map<String, String> body = message.getValue();
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
                    log.debug("미체결 메시지 Kafka 전송 완료 - {}: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("미체결 메시지 Kafka 전송 실패 - {}: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("미체결 메시지 처리 오류 - {}: {}", tradingPair.getSymbol(), e.getMessage(), e);
        }
    }

    /**
     * 부분체결 메시지 처리 및 Kafka로 전송
     */
    private void processPartialMatchMessage(MapRecord<String, String, String> message, TradingPair tradingPair) {
        try {
            String messageId = message.getId().getValue();
            Map<String, String> body = message.getValue();
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
                    log.debug("부분체결 메시지 Kafka 전송 완료 - {}: {}", tradingPair.getSymbol(), messageId);
                } else {
                    log.error("부분체결 메시지 Kafka 전송 실패 - {}: {}", tradingPair.getSymbol(), messageId, ex);
                }
            });
        } catch (Exception e) {
            log.error("부분체결 메시지 처리 오류 - {}: {}", tradingPair.getSymbol(), e.getMessage(), e);
        }
    }

    /**
     * 콜드 데이터 요청 처리
     */
    private void processColdDataRequest(MapRecord<String, String, String> message, TradingPair tradingPair) {
        String messageId = message.getId().getValue();
        Map<String, String> body = message.getValue();
        String streamKey = tradingPair.getColdDataRequestStreamKey();

        String orderType = body.get("orderType");

        try {
            // 콜드 데이터 로드
            loadColdData(tradingPair, orderType);

            // 완료 상태로 변경
            String statusKey = tradingPair.getColdDataStatusKey();
            redisTemplate.opsForValue().set(statusKey, "COMPLETED", 60, TimeUnit.SECONDS);

            // 대기 주문 처리
            processPendingOrders(tradingPair);

            // 메시지 확인
            redisTemplate.opsForStream().acknowledge(streamKey, "cold-data-processor-group", messageId);

            log.info("콜드 데이터 처리 완료 - {}: {}", tradingPair.getSymbol(), orderType);

        } catch (Exception e) {
            log.error("콜드 데이터 처리 중 오류 발생 - {}: {}", tradingPair.getSymbol(), e.getMessage(), e);

            // 오류 발생 시 상태 초기화
            String statusKey = tradingPair.getColdDataStatusKey();
            redisTemplate.delete(statusKey);
        }
    }

    private void loadColdData(TradingPair tradingPair, String orderType) {
        log.info("콜드 데이터 로드 시작 - {}: {}", tradingPair.getSymbol(), orderType);
        // TODO: DB에서 콜드 데이터 로드 로직 구현
    }

    private void processPendingOrders(TradingPair tradingPair) {
        log.info("대기 주문 처리 시작 - {}", tradingPair.getSymbol());
        // TODO: 대기 주문 처리 로직 구현
    }
}