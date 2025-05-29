package com.exchange.order_completed.infrastructure.cassandra.repository;

import com.exchange.order_completed.domain.cassandra.entity.UnmatchedOrder;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.UUID;

public interface ReactiveUnmatchedOrderReaderRepository extends ReactiveCassandraRepository<UnmatchedOrder, UUID> {

    @Query("SELECT * FROM unmatched_order WHERE user_id = :userId AND shard IN (:shard1, :shard2, :shard3) AND year_month_date >= :fromDate AND year_month_date <= :toDate")
    Flux<UnmatchedOrder> findByUserIdAndShardInAndYearMonthDateRange(UUID userId, int shard1, int shard2, int shard3, LocalDate fromDate, LocalDate toDate);
}
