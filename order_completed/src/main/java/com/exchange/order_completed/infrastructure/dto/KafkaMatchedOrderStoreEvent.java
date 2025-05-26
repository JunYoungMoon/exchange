package com.exchange.order_completed.infrastructure.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMatchedOrderStoreEvent {

    private String tradingPair;
    private BigDecimal executionPrice;
    private BigDecimal matchedQuantity;
    private UUID buyUserId;
    private UUID sellUserId;
    private UUID buyMatchedOrderId;
    private UUID sellMatchedOrderId;
    private Instant createdAt;
    private LocalDate yearMonthDate;
    private int buyShard;
    private int sellShard;
}