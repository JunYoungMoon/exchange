package com.exchange.order_completed.domain.postgres.entity;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
public class TradeDataInfo {

    private LocalDateTime minute;
    private String pair;

    @Override
    public String toString() {
        return "TradeDataInfo{" +
                "minute=" + minute +
                ", pair='" + pair + '\'' +
                ", firstPrice=" + firstPrice +
                ", lastPrice=" + lastPrice +
                ", maxPrice=" + maxPrice +
                ", minPrice=" + minPrice +
                ", amount=" + amount +
                '}';
    }

    private BigDecimal firstPrice;
    private BigDecimal lastPrice;
    private BigDecimal maxPrice;
    private BigDecimal minPrice;
    private BigDecimal amount;
}
