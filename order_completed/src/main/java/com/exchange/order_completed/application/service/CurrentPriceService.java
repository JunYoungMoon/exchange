package com.exchange.order_completed.application.service;

import com.exchange.order_completed.application.command.CreateMatchedOrderStoreCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class CurrentPriceService {

    private final RedisTemplate<String, String> redisTemplate;
    private final ReactiveStringRedisTemplate reactiveRedisTemplate;

    // Redis 키 접두사 정의
    private static final String CURRENT_PRICE_KEY_PREFIX = "market:current_price:";

    /**
     * 체결 이벤트에서 현재 가격 업데이트
     */
    public Mono<Void> updateCurrentPrice(List<CreateMatchedOrderStoreCommand> matchedOrders) {
        return Flux.fromIterable(matchedOrders)
                .flatMap(order -> {
                    String tradingPair = order.tradingPair();
                    String price = order.price().toString();
                    String redisKey = CURRENT_PRICE_KEY_PREFIX + tradingPair;

                    return reactiveRedisTemplate.opsForValue()
                            .set(redisKey, price)
                            .doOnNext(success -> log.debug("{}의 현재 가격 업데이트: {}", tradingPair, price));
                })
                .then(); // 최종 결과 Mono<Void>
    }

    /**
     * 현재 가격 조회
     */
    public BigDecimal getCurrentPrice(String tradingPair) {
        String priceStr = redisTemplate.opsForValue().get(CURRENT_PRICE_KEY_PREFIX + tradingPair);

        if (priceStr != null) {
            try {
                return new BigDecimal(priceStr);
            } catch (NumberFormatException e) {
                log.error("현재 가격 형식 오류: {}", priceStr);
            }
        }

        return null;
    }
}
