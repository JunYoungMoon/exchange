package com.exchange.matching.infrastructure.redis;

import com.exchange.matching.application.enums.OrderType;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

public class RedisKeyManager {

    private static final String KEY_PREFIX = "v6d";
    private static final String MARKET_PREFIX = "market";

    @Getter
    @Builder
    public static class ClusterKeys {
        private final String oppositeOrderKey;
        private final String currentOrderKey;
        private final String matchStreamKey;
        private final String unmatchStreamKey;
        private final String partialMatchedStreamKey;
        private final String idempotencyKey;
        private final String bidOrderbookKey;
        private final String askOrderbookKey;
        private final String coldDataRequestStreamKey;
        private final String coldDataStatusKey;
        private final String pendingOrdersKey;
        private final String closingPriceKey;
        private final String updateKeyPrefix;
        private final String updateIndexKey;

        public List<String> toKeyList() {
            return Arrays.asList(
                    oppositeOrderKey,
                    currentOrderKey,
                    matchStreamKey,
                    unmatchStreamKey,
                    partialMatchedStreamKey,
                    idempotencyKey,
                    bidOrderbookKey,
                    askOrderbookKey,
                    coldDataRequestStreamKey,
                    coldDataStatusKey,
                    pendingOrdersKey,
                    closingPriceKey,
                    updateKeyPrefix,
                    updateIndexKey
            );
        }
    }

    public static ClusterKeys generateKeys(String tradingPair, OrderType orderType) {
        String hashTag = "{" + tradingPair + "}";

        // 주문 키 결정
        String oppositeOrderKey, currentOrderKey;
        if (OrderType.BUY.equals(orderType)) {
            oppositeOrderKey = buildKey(KEY_PREFIX, hashTag, "orders", "sell");
            currentOrderKey = buildKey(KEY_PREFIX, hashTag, "orders", "buy");
        } else {
            oppositeOrderKey = buildKey(KEY_PREFIX, hashTag, "orders", "buy");
            currentOrderKey = buildKey(KEY_PREFIX, hashTag, "orders", "sell");
        }

        return ClusterKeys.builder()
                .oppositeOrderKey(oppositeOrderKey)
                .currentOrderKey(currentOrderKey)
                .matchStreamKey(buildKey(KEY_PREFIX, hashTag, "stream", "matches"))
                .unmatchStreamKey(buildKey(KEY_PREFIX, hashTag, "stream", "unmatched"))
                .partialMatchedStreamKey(buildKey(KEY_PREFIX, hashTag, "stream", "partialMatched"))
                .idempotencyKey(buildKey(KEY_PREFIX, hashTag, "idempotency", "orders"))
                .bidOrderbookKey(buildKey(KEY_PREFIX, hashTag, "orderbook", "bids"))
                .askOrderbookKey(buildKey(KEY_PREFIX, hashTag, "orderbook", "asks"))
                .coldDataRequestStreamKey(buildKey(KEY_PREFIX, hashTag, "stream", "cold_data_request"))
                .coldDataStatusKey(buildKey(KEY_PREFIX, hashTag, "cold_data_status"))
                .pendingOrdersKey(buildKey(KEY_PREFIX, hashTag, "pending_orders"))
                .closingPriceKey(buildKey(MARKET_PREFIX, hashTag, "closing_price"))
                .updateKeyPrefix(buildKey(KEY_PREFIX, hashTag, "order", "pending-updates"))  // 추가
                .updateIndexKey(buildKey(KEY_PREFIX, hashTag, "order", "pending-updates", "index"))  // 추가
                .build();
    }

    private static String buildKey(String... parts) {
        return String.join(":", parts);
    }
}