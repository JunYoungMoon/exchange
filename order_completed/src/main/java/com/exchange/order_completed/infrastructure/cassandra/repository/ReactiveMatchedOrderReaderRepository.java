package com.exchange.order_completed.infrastructure.cassandra.repository;

import com.exchange.order_completed.domain.cassandra.entity.MatchedOrder;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.repository.Query;

import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.UUID;

public interface ReactiveMatchedOrderReaderRepository extends ReactiveCassandraRepository<MatchedOrder, UUID> {

    @Query("SELECT * FROM matched_order WHERE user_id = :userId AND shard IN (:shard1, :shard2, :shard3) AND year_month_date >= :fromDate AND year_month_date <= :toDate")
    Flux<MatchedOrder> findByUserIdAndShardInAndYearMonthDateRange(UUID userId, int shard1, int shard2, int shard3, LocalDate fromDate, LocalDate toDate);
}