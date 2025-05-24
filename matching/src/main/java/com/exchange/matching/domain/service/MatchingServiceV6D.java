package com.exchange.matching.domain.service;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.application.enums.OrderType;
import com.exchange.matching.infrastructure.redis.RedisKeyManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class MatchingServiceV6D implements MatchingService {

    private final RedisTemplate<String, String> redisTemplate;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final RedisScript<Boolean> matchingScript;

    // 기본 설정값들
    private static final String DEFAULT_PRICE_DIFF_THRESHOLD = "0.3"; // 30%

    @Override
    public MatchingVersion getVersion() {
        return MatchingVersion.V6D;
    }

    public MatchingServiceV6D(RedisTemplate<String, String> redisTemplate,
                              ReactiveRedisTemplate<String, String> reactiveRedisTemplate) {
        this.redisTemplate = redisTemplate;
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

    public void matchOrders(CreateMatchingCommand command) {
        MatchingOrder matchingOrder = MatchingOrder.fromCommand(command);

        log.info("{} 주문접수 : {}원 {}개 (주문ID: {})",
                matchingOrder.getOrderType(), matchingOrder.getPrice(),
                matchingOrder.getQuantity(), matchingOrder.getUserId());

        matchingProcess(matchingOrder);
    }

    @Override
    public Mono<Void> matchOrdersReactive(CreateMatchingCommand command) {
        MatchingOrder matchingOrder = MatchingOrder.fromCommand(command);

        log.info("{} 주문접수 (리액티브) : {}원 {}개 (주문ID: {})",
                matchingOrder.getOrderType(), matchingOrder.getPrice(),
                matchingOrder.getQuantity(), matchingOrder.getUserId());

        return matchingProcessReactive(matchingOrder);
    }

    /**
     * 주문 매칭 프로세스 시작
     */
    private void matchingProcess(MatchingOrder order) {
        if (order.getTimestamp() == null) {
            order.setTimestamp(System.currentTimeMillis());
        }

        // 클러스터 키 생성 - 해시 태그로 같은 노드에 저장
        RedisKeyManager.ClusterKeys keys = RedisKeyManager.generateKeys(
                order.getTradingPair(), order.getOrderType());

        // 주문 정보 직렬화
        String orderDetails = serializeOrder(order);
        String partialOrderId = UUID.randomUUID().toString();

        // 인자 준비 (8개)
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

        // Lua 스크립트 실행
        redisTemplate.execute(matchingScript, keys.toKeyList(), args.toArray());

    }

    private Mono<Void> matchingProcessReactive(MatchingOrder order) {
        if (order.getTimestamp() == null) {
            order.setTimestamp(System.currentTimeMillis());
        }

        // 클러스터 키 생성
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

        return reactiveRedisTemplate
                .execute(matchingScript, keys.toKeyList(), args)
                .then();
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