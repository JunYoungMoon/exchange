package com.exchange.receive.infrastructure.schedule;

import com.exchange.receive.infrastructure.cassandra.ShardCalculator;
import com.exchange.receive.infrastructure.dto.KafkaOrderStoreEvent;
import com.exchange.receive.infrastructure.enums.OperationType;
import com.exchange.receive.infrastructure.enums.OrderType;
import com.exchange.receive.infrastructure.enums.TradingPair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderUpdateScheduler {

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ShardCalculator shardCalculator;

    private static final String UNMATCH_KAFKA_TOPIC = "matching-to-order_completed.execute-order-unmatched";

    // Lua 스크립트 상수 정의
    private static final String DELETE_IF_VERSION_MATCHES_SCRIPT =
            "local current = redis.call('HGET', KEYS[1], 'version') " +
                    "local currentVersion = tonumber(current) or 1 " +
                    "if currentVersion == tonumber(ARGV[1]) then " +
                    "  redis.call('DEL', KEYS[1]) " +
                    "  redis.call('SREM', KEYS[2], ARGV[2]) " +
                    "  return 1 " +
                    "else " +
                    "  return 0 " +
                    "end";

    @Scheduled(fixedDelay = 3000)
    public void processOrderUpdates() {
        try {
            log.debug("클러스터 주문 업데이트 처리 시작");

            // 각 거래쌍별로 독립적으로 처리
            for (TradingPair tradingPair : TradingPair.values()) {
                processOrderUpdatesForTradingPair(tradingPair);
            }

        } catch (Exception e) {
            log.error("클러스터 주문 업데이트 스케줄러 오류", e);
        }
    }

    /**
     * 특정 거래쌍의 주문 업데이트 처리
     */
    private void processOrderUpdatesForTradingPair(TradingPair tradingPair) {
        try {
            // 거래쌍별 키 생성
            String updateKeyPrefix = tradingPair.createHashTagKey("v6d", "order", "pending-updates") + ":";
            String updateIndexKey = tradingPair.createHashTagKey("v6d", "order", "pending-updates", "index");

            // 1. Pending updates 인덱스 조회
            Set<String> orderIds = redisTemplate.opsForSet().members(updateIndexKey);

            if (orderIds == null || orderIds.isEmpty()) {
                log.debug("거래쌍 {} - 처리할 주문 업데이트 없음", tradingPair.getSymbol());
                return;
            }

            log.info("거래쌍 {} - 주문 업데이트 처리 시작: {} 건", tradingPair.getSymbol(), orderIds.size());

            // 2. 파이프라인으로 모든 주문 데이터 일괄 조회
            Map<String, Map<String, String>> orderDataMap = fetchOrderDataInBatch(orderIds, updateKeyPrefix);

            // 3. 성공적으로 Kafka로 전송된 주문들의 ID와 버전을 추적
            Map<String, Integer> successfulOrders = new ConcurrentHashMap<>();

            // 4. CompletableFuture를 사용하여 Kafka로 주문 이벤트 전송 (비동기)
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (Map.Entry<String, Map<String, String>> entry : orderDataMap.entrySet()) {
                String orderId = entry.getKey();
                Map<String, String> orderData = entry.getValue();

                try {
                    // 거래쌍 검증 (안전장치)
                    String orderTradingPair = orderData.get("tradingPair");
                    if (!tradingPair.getSymbol().equals(orderTradingPair)) {
                        log.warn("거래쌍 불일치 - 예상: {}, 실제: {}, 주문ID: {}",
                                tradingPair.getSymbol(), orderTradingPair, orderId);
                        continue;
                    }

                    // 버전 정보 추출
                    int version = Integer.parseInt(orderData.getOrDefault("version", "1"));

                    long timestamp = Long.parseLong(orderData.get("timestamp"));
                    long processedTimestamp = "BUY".equals(orderData.get("orderType"))
                            ? 9999999999999L - timestamp
                            : timestamp;

                    Instant instant = Instant.ofEpochMilli(processedTimestamp);
                    LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();

                    // 이벤트 객체 생성
                    KafkaOrderStoreEvent event = KafkaOrderStoreEvent.builder()
                            .tradingPair(orderData.get("tradingPair"))
                            .orderType(OrderType.valueOf(orderData.get("orderType")))
                            .price(new BigDecimal(orderData.get("price")))
                            .quantity(new BigDecimal(orderData.get("quantity")))
                            .userId(UUID.fromString(orderData.get("userId")))
                            .orderId(UUID.fromString(orderData.get("orderId")))
                            .startTime(Long.parseLong(orderData.get("timestamp")))
                            .operationType(OperationType.valueOf(orderData.get("operation")))
                            .shard(shardCalculator.calculateShard(UUID.fromString(orderData.get("orderId"))))
                            .yearMonthDate(localDate)
                            .createdAt(instant)
                            .build();

                    // Kafka로 전송 및 결과 처리를 위한 CompletableFuture 체인 생성
                    CompletableFuture<Void> future = kafkaTemplate.send(UNMATCH_KAFKA_TOPIC, orderId, event)
                            .thenAccept(result -> {
                                log.debug("거래쌍 {} - 주문 업데이트 전송 성공: {}, operation: {}, version: {}",
                                        tradingPair.getSymbol(), orderId, orderData.get("operation"), version);

                                // 성공적으로 전송된 주문 추적
                                successfulOrders.put(orderId, version);
                            })
                            .exceptionally(ex -> {
                                log.error("거래쌍 {} - 주문 업데이트 전송 실패: {}", tradingPair.getSymbol(), orderId, ex);
                                return null;
                            });

                    futures.add(future);

                } catch (Exception e) {
                    log.error("거래쌍 {} - 주문 업데이트 처리 중 오류 발생: {}", tradingPair.getSymbol(), orderId, e);
                    // 예외 상황도 처리 완료된 것으로 간주하는 CompletableFuture 추가
                    futures.add(CompletableFuture.completedFuture(null));
                }
            }

            // 5. 모든 CompletableFuture가 완료될 때까지 대기 (타임아웃 적용)
            try {
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                        futures.toArray(new CompletableFuture[0]));

                allFutures.get(5, TimeUnit.SECONDS); // 최대 5초 대기
                log.debug("거래쌍 {} - 모든 Kafka 전송 작업이 완료되었습니다.", tradingPair.getSymbol());

            } catch (TimeoutException e) {
                log.warn("거래쌍 {} - 일부 Kafka 전송 작업이 시간 내에 완료되지 않았습니다.", tradingPair.getSymbol());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("거래쌍 {} - Kafka 전송 완료 대기 중 인터럽트 발생", tradingPair.getSymbol(), e);
            } catch (ExecutionException e) {
                log.error("거래쌍 {} - Kafka 전송 작업 중 실행 오류 발생", tradingPair.getSymbol(), e);
            }

            // 6. 성공적으로 전송된 주문들에 대해 일괄 삭제 처리
            if (!successfulOrders.isEmpty()) {
                deleteOrdersInBatch(successfulOrders, updateKeyPrefix, updateIndexKey, tradingPair);
            }

            log.info("거래쌍 {} - 주문 업데이트 처리 완료: 총 {} 건 중 {} 건 성공",
                    tradingPair.getSymbol(), orderDataMap.size(), successfulOrders.size());

        } catch (Exception e) {
            log.error("거래쌍 {} - 주문 업데이트 처리 중 오류", tradingPair.getSymbol(), e);
        }
    }

    /**
     * 주문 데이터를 파이프라인으로 일괄 조회
     */
    private Map<String, Map<String, String>> fetchOrderDataInBatch(Set<String> orderIds, String updateKeyPrefix) {
        Map<String, Map<String, String>> orderDataMap = new HashMap<>();

        // executePipelined를 사용해서 update/delete 데이터 상세 조회
        List<Object> results = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            RedisHashCommands hashCommands = connection.hashCommands();

            // 각 주문 ID에 대해 RedisHashCommands의 hGetAll 명령 실행
            for (String orderId : orderIds) {
                byte[] keyBytes = (updateKeyPrefix + orderId).getBytes(StandardCharsets.UTF_8);
                hashCommands.hGetAll(keyBytes);
            }

            return null;
        });

        Iterator<String> orderIdIterator = orderIds.iterator();

        // orderIds와 results를 비교해보고
        for (Object result : results) {
            if (!orderIdIterator.hasNext()) {
                log.warn("orderIds 개수보다 results 개수가 많습니다");
                break;
            }

            String orderId = orderIdIterator.next();

            @SuppressWarnings("unchecked")
            Map<String, String> orderData = (Map<String, String>) result;

            if (orderData != null && !orderData.isEmpty()) {
                orderDataMap.put(orderId, orderData);
            }
        }

        if (orderIdIterator.hasNext()) {
            log.warn("results 개수보다 orderIds 개수가 많습니다");
        }

        return orderDataMap;
    }

    /**
     * 주문 데이터를 파이프라인으로 일괄 삭제
     */
    private void deleteOrdersInBatch(Map<String, Integer> orderVersionMap, String updateKeyPrefix,
                                     String updateIndexKey, TradingPair tradingPair) {
        if (orderVersionMap.isEmpty()) {
            return;
        }

        log.info("거래쌍 {} - 일괄 주문 업데이트 삭제 시작: {} 건", tradingPair.getSymbol(), orderVersionMap.size());

        // 파이프라인으로 Lua 스크립트 일괄 실행
        List<Object> results = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            RedisScriptingCommands scriptingCommands = connection.scriptingCommands();

            for (Map.Entry<String, Integer> entry : orderVersionMap.entrySet()) {
                String orderId = entry.getKey();
                Integer expectedVersion = entry.getValue();
                String updateKey = updateKeyPrefix + orderId;

                // 각 주문에 대해 Lua 스크립트 실행
                byte[] script = DELETE_IF_VERSION_MATCHES_SCRIPT.getBytes(StandardCharsets.UTF_8);

                // 키와 인자를 개별적으로 전달
                scriptingCommands.eval(
                        script,
                        ReturnType.INTEGER,
                        2,  // numKeys: KEYS 배열의 개수
                        updateKey.getBytes(StandardCharsets.UTF_8),           // KEYS[1]
                        updateIndexKey.getBytes(StandardCharsets.UTF_8),     // KEYS[2]
                        String.valueOf(expectedVersion).getBytes(StandardCharsets.UTF_8), // ARGV[1]
                        orderId.getBytes(StandardCharsets.UTF_8)            // ARGV[2]
                );
            }
            return null;
        });

        // 결과 처리
        int successCount = 0;
        int index = 0;
        for (Map.Entry<String, Integer> entry : orderVersionMap.entrySet()) {
            String orderId = entry.getKey();
            Integer expectedVersion = entry.getValue();

            if (index < results.size()) {
                Long result = (Long) results.get(index++);
                boolean success = (result != null && result == 1);

                if (success) {
                    successCount++;
                    log.debug("거래쌍 {} - 주문 업데이트 삭제 성공: {}, version: {}",
                            tradingPair.getSymbol(), orderId, expectedVersion);
                } else {
                    log.debug("거래쌍 {} - 버전 불일치로 삭제 실패: {}, expected: {}",
                            tradingPair.getSymbol(), orderId, expectedVersion);
                }
            }
        }

        log.info("거래쌍 {} - 일괄 주문 업데이트 삭제 완료: 총 {}건 중 {}건 성공",
                tradingPair.getSymbol(), orderVersionMap.size(), successCount);
    }
}