package com.exchange.order.application.service;

import com.exchange.order.application.command.CreateOrderCommand;
import com.exchange.order.application.result.FindCancelResult;
import com.exchange.order.application.result.FindOrderResult;
import com.exchange.order.infrastructure.dto.KafkaOrderCancelEvent;
import com.exchange.order.infrastructure.dto.KafkaUserBalanceDecreaseEvent;
import com.exchange.order.infrastructure.enums.TradingPair;
import com.exchange.order.presentation.request.CancelOrderRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
public class OrderCommandService {

    private final KafkaTemplate<String, KafkaUserBalanceDecreaseEvent> decreaseKafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final RedisScript<String> removeOrderScript;

    private static final String TOPIC_USER_BALANCE_DECREASE = "order-to-user.execute-decrease-balance";

    /**
     * 주문 생성
     */
    public FindOrderResult createOrder(CreateOrderCommand command) {
        KafkaUserBalanceDecreaseEvent kafkaUserBalanceDecreaseEvent =
                KafkaUserBalanceDecreaseEvent.fromCommand(command);

        decreaseKafkaTemplate.send(TOPIC_USER_BALANCE_DECREASE, kafkaUserBalanceDecreaseEvent);

        log.info("주문 생성 요청 전송 완료 - 거래쌍: {}, 주문ID: {}",
                command.getTradingPair(), command.getOrderId());

        return FindOrderResult.fromResult(kafkaUserBalanceDecreaseEvent);
    }

    /**
     * 주문 취소
     */
    public FindCancelResult cancelOrder(UUID userId, CancelOrderRequest request) {
        // 1. 유저 권한 확인
        if (!userId.equals(request.getUserId())) {
            throw new IllegalArgumentException("주문 취소 권한이 없습니다.");
        }

        // 2. 거래쌍 파싱 및 검증
        TradingPair tradingPair;
        try {
            tradingPair = TradingPair.fromSymbol(request.getTradingPair());
        } catch (IllegalArgumentException e) {
            log.error("지원하지 않는 거래쌍: {}", request.getTradingPair());
            throw new IllegalArgumentException("지원하지 않는 거래쌍입니다: " + request.getTradingPair());
        }

        // 3. 클러스터 키 생성 (해시 태그 포함)
        String zsetKey = generateOrderKey(tradingPair, String.valueOf(request.getOrderType()));
        String cancelStreamKey = tradingPair.createHashTagKey("v6d", "stream", "cancel");

        // 4. 주문 직렬화
        String serializedOrder = serializeOrderForCancel(
                request.getTimestamp(),
                request.getQuantity(),
                request.getUserId(),
                request.getOrderId()
        );

        log.info("주문 취소 시도 - 거래쌍: {}, 주문ID: {}, 키: {}",
                tradingPair.getSymbol(), request.getOrderId(), zsetKey);
        log.debug("직렬화된 주문 데이터: '{}' (길이: {})", serializedOrder, serializedOrder.length());

        // 5. Lua 스크립트로 미체결 주문 제거
        List<String> keys = Arrays.asList(zsetKey, cancelStreamKey);
        String orderType = String.valueOf(request.getOrderType());

        try {
            String result = redisTemplate.execute(
                    removeOrderScript,
                    keys,
                    serializedOrder,
                    tradingPair.getSymbol(),
                    orderType
            );

            if (result != null && !result.isEmpty()) {
                log.info("주문 취소 성공 - 거래쌍: {}, 주문ID: {}, 결과: {}",
                        tradingPair.getSymbol(), request.getOrderId(), result);
            } else {
                log.warn("주문 취소 실패 - 거래쌍: {}, 주문ID: {} (주문을 찾을 수 없음)",
                        tradingPair.getSymbol(), request.getOrderId());
            }

        } catch (Exception e) {
            log.error("주문 취소 중 오류 발생 - 거래쌍: {}, 주문ID: {}",
                    tradingPair.getSymbol(), request.getOrderId(), e);
            throw new RuntimeException("주문 취소 처리 중 오류가 발생했습니다.", e);
        }

        // 6. 주문 취소 결과 반환
        return FindCancelResult.fromResult(request);
    }

    /**
     * 거래쌍과 주문 타입에 따른 클러스터 키 생성
     */
    private String generateOrderKey(TradingPair tradingPair, String orderType) {
        if ("BUY".equals(orderType)) {
            return tradingPair.createHashTagKey("v6d", "orders", "buy");
        } else {
            return tradingPair.createHashTagKey("v6d", "orders", "sell");
        }
    }

    /**
     * 주문 취소를 위한 주문 데이터 직렬화
     * 형식: timestamp|quantity|userId|orderId
     */
    private String serializeOrderForCancel(long timestamp, BigDecimal quantity, UUID userId, UUID orderId) {
        String timeStr = String.format("%013d", timestamp);
        return timeStr + "|" + quantity + "|" + userId + "|" + orderId;
    }
}