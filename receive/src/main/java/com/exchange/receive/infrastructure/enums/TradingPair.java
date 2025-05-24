package com.exchange.receive.infrastructure.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum TradingPair {
    BTC_KRW("BTC/KRW"),
    ETH_KRW("ETH/KRW");

    private final String symbol;

    public String createHashTagKey(String prefix, String... suffixes) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(prefix).append(":{").append(this.symbol).append("}");

        for (String suffix : suffixes) {
            keyBuilder.append(":").append(suffix);
        }

        return keyBuilder.toString();
    }

    /**
     * Stream 키 생성
     */
    public String getMatchStreamKey() {
        return createHashTagKey("v6d", "stream", "matches");
    }

    public String getUnmatchStreamKey() {
        return createHashTagKey("v6d", "stream", "unmatched");
    }

    public String getPartialMatchedStreamKey() {
        return createHashTagKey("v6d", "stream", "partialMatched");
    }

    public String getColdDataRequestStreamKey() {
        return createHashTagKey("v6d", "stream", "cold_data_request");
    }

    public String getColdDataStatusKey() {
        return createHashTagKey("v6d", "cold_data_status");
    }

    public String getPendingOrdersKey() {
        return createHashTagKey("v6d", "pending_orders");
    }
}