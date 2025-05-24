package com.exchange.order.infrastructure.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum TradingPair {
    BTC_KRW("BTC/KRW"),
    ETH_KRW("ETH/KRW");

    private final String symbol;

    /**
     * 문자열로부터 TradingPair 찾기
     */
    public static TradingPair fromSymbol(String symbol) {
        for (TradingPair pair : values()) {
            if (pair.symbol.equals(symbol)) {
                return pair;
            }
        }
        throw new IllegalArgumentException("Unknown trading pair: " + symbol);
    }

    /**
     * 해시 태그 포함한 키 생성
     * 예: v6d:{BTC/KRW}:orders:buy
     */
    public String createHashTagKey(String prefix, String... suffixes) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(prefix).append(":{").append(this.symbol).append("}");

        for (String suffix : suffixes) {
            keyBuilder.append(":").append(suffix);
        }

        return keyBuilder.toString();
    }

    /**
     * 주문 키 생성 메서드들
     */
    public String getBuyOrderKey() {
        return createHashTagKey("v6d", "orders", "buy");
    }

    public String getSellOrderKey() {
        return createHashTagKey("v6d", "orders", "sell");
    }

    public String getCancelStreamKey() {
        return createHashTagKey("v6d", "stream", "cancel");
    }

    public String getMatchStreamKey() {
        return createHashTagKey("v6d", "stream", "matches");
    }

    public String getUnmatchStreamKey() {
        return createHashTagKey("v6d", "stream", "unmatched");
    }
}