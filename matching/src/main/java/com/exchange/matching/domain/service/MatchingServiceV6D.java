package com.exchange.matching.domain.service;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.enums.OrderType;
import com.exchange.matching.common.exception.RedisBackpressureException;
import com.exchange.matching.common.exception.RedisCircuitBreakerException;
import com.exchange.matching.infrastructure.redis.RedisKeyManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class MatchingServiceV6D implements MatchingService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final RedisScript<Boolean> matchingScript;

    // 기본 설정값들
    private static final String DEFAULT_PRICE_DIFF_THRESHOLD = "0.3"; // 30%

    // Redis 백프레셔 관련 설정
    private final AtomicInteger activeRedisOperations = new AtomicInteger(0);
    private final AtomicLong lastRedisErrorTime = new AtomicLong(0);
    private final AtomicInteger consecutiveRedisErrors = new AtomicInteger(0);

    @Value("${redis.max-concurrent-operations:50}")
    private int maxConcurrentRedisOperations;

    @Value("${redis.operation-timeout-ms:5000}")
    private long redisOperationTimeoutMs;

    @Value("${redis.circuit-breaker-threshold:10}")
    private int redisCircuitBreakerThreshold;

    @Value("${redis.circuit-breaker-timeout-ms:30000}")
    private long redisCircuitBreakerTimeoutMs;

    @Override
    public MatchingVersion getVersion() {
        return MatchingVersion.V6D;
    }

    public MatchingServiceV6D(ReactiveRedisTemplate<String, String> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;

        // Lua 스크립트 로드
        DefaultRedisScript<Boolean> script = new DefaultRedisScript<>();
        script.setResultType(Boolean.class);
        try {
            ClassPathResource resource = new ClassPathResource("scripts/matchingV6D.lua");
            String scriptText = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            script.setScriptText(scriptText);
        } catch (IOException e) {
            log.error("Lua 스크립트 로드 실패", e);
            throw new RuntimeException("Lua 스크립트 로드 실패", e);
        }
        this.matchingScript = script;
    }

    public void matchOrders(CreateMatchingCommand command) {}

    @Override
    public Mono<Void> matchOrdersReactive(CreateMatchingCommand command) {
        MatchingOrder matchingOrder = MatchingOrder.fromCommand(command);

        log.info("{} 주문접수 (리액티브) : {}원 {}개 (주문ID: {})",
                matchingOrder.getOrderType(), matchingOrder.getPrice(),
                matchingOrder.getQuantity(), matchingOrder.getUserId());

        return matchingProcessReactiveWithBackpressure(matchingOrder);
    }

    private Mono<Void> matchingProcessReactiveWithBackpressure(MatchingOrder order) {
        return Mono.defer(() -> {
                    // 1. Redis Circuit Breaker 체크
                    if (isRedisCircuitBreakerOpen()) {
                        log.warn("Redis Circuit Breaker 열림 - 주문 처리 지연");
                        return Mono.error(new RedisCircuitBreakerException("Redis 서비스 일시 중단"));
                    }

                    // 2. 동시 Redis 연산 수 체크
                    if (activeRedisOperations.get() >= maxConcurrentRedisOperations) {
                        log.warn("Redis 동시 연산 한계 도달 - 대기 중: {}/{}",
                                activeRedisOperations.get(), maxConcurrentRedisOperations);
                        return Mono.error(new RedisBackpressureException("Redis 백프레셔 발생"));
                    }

                    return executeRedisMatchingWithMonitoring(order);
                })
                // 3. 백프레셔 발생 시 재시도 로직
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .maxBackoff(Duration.ofSeconds(1))
                        .filter(this::isRedisRetryableError)
                        .doBeforeRetry(retrySignal ->
                                log.warn("Redis 백프레셔로 인한 재시도: {}/3", retrySignal.totalRetries() + 1)))

                // 4. 최종 실패 시 처리
                .onErrorResume(error -> {
                    if (error instanceof RedisBackpressureException ||
                            error instanceof RedisCircuitBreakerException) {
                        log.error("Redis 백프레셔 처리 실패 - 주문: {}", order.getOrderId(), error);
                        // 여기서 다른 처리 방식으로 폴백하거나 지연 큐에 추가할 수 있음
                        return handleRedisBackpressureFailure(order, error);
                    }
                    return Mono.error(error);
                });
    }

    private Mono<Void> handleRedisBackpressureFailure(MatchingOrder order, Throwable error) {
        log.error("Redis 백프레셔 최종 실패 - Kafka 재시도로 위임: 주문ID={}, 에러={}",
                order.getOrderId(), error.getMessage());

        // Redis 실패를 Kafka 레벨에서 처리하도록 예외 전파
        // 이렇게 하면 ReactiveEventConsumer의 재시도 로직이 동작함
        return Mono.error(new RedisProcessingFailureException(
                "Redis 백프레셔로 인한 주문 처리 실패 - Kafka 재시도 필요",
                error, order.getOrderId()));
    }

    /**
     * Redis 처리 실패를 나타내는 예외
     * Kafka Consumer에서 재시도 가능한 예외로 처리됨
     */
    @Getter
    public static class RedisProcessingFailureException extends RuntimeException {
        private final UUID orderId;

        public RedisProcessingFailureException(String message, Throwable cause, UUID orderId) {
            super(message, cause);
            this.orderId = orderId;
        }

        @Override
        public String getMessage() {
            return super.getMessage() + " [OrderId: " + orderId + "]";
        }
    }

    private boolean isRedisRetryableError(Throwable throwable) {
        // Redis 백프레셔 관련 재시도 가능한 에러들
        return throwable instanceof RedisBackpressureException ||
                throwable instanceof RedisCircuitBreakerException ||
                throwable instanceof java.util.concurrent.TimeoutException ||
                (throwable.getCause() != null &&
                        throwable.getCause().getMessage() != null &&
                        (throwable.getCause().getMessage().contains("timeout") ||
                                throwable.getCause().getMessage().contains("connection") ||
                                throwable.getCause().getMessage().contains("busy")));
    }

    /**
     * 주문 매칭 프로세스 시작
     */
    private Mono<Void> executeRedisMatchingWithMonitoring(MatchingOrder order) {
        if (order.getTimestamp() == null) {
            order.setTimestamp(System.currentTimeMillis());
        }

        RedisKeyManager.ClusterKeys keys = RedisKeyManager.generateKeys(
                order.getTradingPair(), order.getOrderType());

        String orderDetails = serializeOrder(order);
        String partialOrderId = UUID.randomUUID().toString();

        List<String> args = Arrays.asList(
                order.getOrderType().toString(),
                order.getPrice().toString(),
                order.getQuantity().toString(),
                orderDetails,
                order.getTradingPair(),
                order.getOrderId().toString(),
                partialOrderId,
                DEFAULT_PRICE_DIFF_THRESHOLD
        );

        // 동시 연산 수 증가
        activeRedisOperations.incrementAndGet();

        return reactiveRedisTemplate
                .execute(matchingScript, keys.toKeyList(), args)
                .timeout(Duration.ofMillis(redisOperationTimeoutMs))
                .then()
                // 성공 시 에러 카운터 리셋
                .doOnSuccess(v -> {
                    consecutiveRedisErrors.set(0);
                    activeRedisOperations.decrementAndGet();
                })
                // 에러 시 카운터 증가
                .doOnError(error -> {
                    incrementRedisErrorCount();
                    activeRedisOperations.decrementAndGet();
                })
                // 취소 시에도 카운터 감소
                .doOnCancel(activeRedisOperations::decrementAndGet);
    }

    private boolean isRedisCircuitBreakerOpen() {
        if (consecutiveRedisErrors.get() >= redisCircuitBreakerThreshold) {
            long timeSinceLastError = System.currentTimeMillis() - lastRedisErrorTime.get();
            if (timeSinceLastError > redisCircuitBreakerTimeoutMs) {
                log.info("Redis Circuit Breaker 복구 - 에러 카운터 리셋");
                consecutiveRedisErrors.set(0);
                return false;
            }
            return true;
        }
        return false;
    }

    private void incrementRedisErrorCount() {
        int errorCount = consecutiveRedisErrors.incrementAndGet();
        lastRedisErrorTime.set(System.currentTimeMillis());

        if (errorCount >= redisCircuitBreakerThreshold) {
            log.error("Redis Circuit Breaker 활성화 - 연속 에러: {}, {}ms 후 재시도",
                    errorCount, redisCircuitBreakerTimeoutMs);
        }
    }

    /**
     * 주문 직렬화
     * 형식: timestamp|quantity|userId|orderId
     */
    private String serializeOrder(MatchingOrder order) {
        String timeStr;
        if (order.getOrderType() == OrderType.BUY) {
            // 반전된 타임스탬프 사용
            timeStr = String.format("%013d", 9999999999999L - order.getTimestamp());
        } else {
            // 일반 타임스탬프 사용
            timeStr = String.format("%013d", order.getTimestamp());
        }

        return timeStr + "|" +
                order.getQuantity() + "|" +
                order.getUserId() + "|" +
                order.getOrderId();
    }

    /**
     * 주문 매칭 프로세스에서 사용하는 내부 DTO 클래스
     */
    @Setter
    @Getter
    @AllArgsConstructor
    public static class MatchingOrder {
        private final String tradingPair;
        private OrderType orderType;
        private BigDecimal price;
        private Long timestamp;
        private BigDecimal quantity;
        private UUID userId;
        private UUID orderId;

        public MatchingOrder(String tradingPair, OrderType orderType, BigDecimal price,
                             BigDecimal quantity, UUID userId, UUID orderId) {
            this.tradingPair = tradingPair;
            this.orderType = orderType;
            this.price = price;
            this.timestamp = null;
            this.quantity = quantity;
            this.userId = userId;
            this.orderId = orderId;
        }

        public static MatchingOrder fromCommand(CreateMatchingCommand command) {
            return new MatchingOrder(
                    command.tradingPair(),
                    command.orderType(),
                    command.price(),
                    command.quantity(),
                    command.userId(),
                    command.orderId()
            );
        }
    }
}