package com.exchange.order_completed.infrastructure.dto;

import com.exchange.order_completed.domain.cassandra.entity.OrderType;
import com.exchange.order_completed.infrastructure.enums.OperationType;
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
public class KafkaUnmatchedOrderStoreEvent {

    private String tradingPair;
    private OrderType orderType;
    private BigDecimal price;
    private BigDecimal quantity;
    private UUID userId;
    private UUID orderId;
    private OperationType operationType;
    private int shard;
    private LocalDate yearMonthDate;
    private long startTime;
    private Instant createdAt;
}