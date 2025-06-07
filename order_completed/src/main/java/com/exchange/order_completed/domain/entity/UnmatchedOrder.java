package com.exchange.order_completed.domain.entity;

import com.exchange.order_completed.domain.cassandra.entity.OrderState;
import com.exchange.order_completed.infrastructure.enums.OrderType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "unmatched_order")
public class UnmatchedOrder {
    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "shard", nullable = false)
    private Integer shard;

    @Column(name = "year_month_date", nullable = false)
    private LocalDate yearMonthDate;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "order_state", length = 50)
    private OrderState orderState;

    @Column(name = "order_type", length = 50)
    private OrderType orderType;

    @Column(name = "price", precision = 20, scale = 8)
    private BigDecimal price;

    @Column(name = "quantity", precision = 20, scale = 8)
    private BigDecimal quantity;

    @Column(name = "trading_pair", length = 20)
    private String tradingPair;
}
