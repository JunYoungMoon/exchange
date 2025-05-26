package com.exchange.order_completed.infrastructure.cassandra.repository;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.exchange.order_completed.domain.cassandra.entity.MatchedOrder;
import com.exchange.order_completed.domain.cassandra.entity.UnmatchedOrder;
import com.exchange.order_completed.domain.cassandra.repository.MatchedOrderStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MatchedOrderStoreImpl implements MatchedOrderStore {

    private final MatchedOrderStoreRepository matchedOrderStoreRepository;
    private final ReactiveCqlTemplate reactiveCqlTemplate;
    private final CassandraTemplate cassandraTemplate;
    private final CqlTemplate cqlTemplate;

    @Override
    public Mono<Void> saveBatch(List<MatchedOrder> matchedOrderList) {
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);

        for (MatchedOrder order : matchedOrderList) {
            SimpleStatement stmt = SimpleStatement.builder(
                            "INSERT INTO matched_order (user_id, shard, year_month_date, matched_order_id, price, quantity, order_type, trading_pair, created_at) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .addPositionalValues(
                            order.getUserId(),
                            order.getShard(),
                            order.getYearMonthDate(),
                            order.getMatchedOrderId(),
                            order.getPrice(),
                            order.getQuantity(),
                            order.getOrderType().name(),
                            order.getTradingPair(),
                            order.getCreatedAt()
                    )
                    .build();

            batch.addStatement(stmt);
        }

        return reactiveCqlTemplate.execute(batch.build()).then();
    }

    @Override
    public void save(MatchedOrder matchedOrder) {
        matchedOrderStoreRepository.save(matchedOrder);
    }

    @Override
    public void saveMatchedOrderAndUpdateUnmatchedOrder(MatchedOrder matchedOrder, UnmatchedOrder unmatchedOrder) {
        // 1. INSERT INTO matched_order
        SimpleStatement insertMatchedOrderStatement = SimpleStatement.builder(
                        "INSERT INTO matched_order (user_id, shard, year_month_date, idempotency_id, created_at, order_id, price, quantity, order_type, trading_pair) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .addPositionalValues(
                        matchedOrder.getUserId(),
                        matchedOrder.getShard(),
                        matchedOrder.getYearMonthDate(),
                        matchedOrder.getMatchedOrderId(),
                        matchedOrder.getCreatedAt(),
                        matchedOrder.getPrice(),
                        matchedOrder.getQuantity(),
                        matchedOrder.getOrderType(),
                        matchedOrder.getTradingPair()
                )
                .build();

        // 2. UPDATE 또는 DELETE 결정
        SimpleStatement unmatchedOrderStatement;

        if (unmatchedOrder.getQuantity().compareTo(BigDecimal.ZERO) == 0) {
            // quantity == 0 이면 DELETE
            unmatchedOrderStatement = SimpleStatement.builder(
                            "DELETE FROM unmatched_order WHERE user_id = ? AND shard = ? AND year_month_date = ? AND order_id = ?")
                    .addPositionalValues(
                            unmatchedOrder.getUserId(),
                            unmatchedOrder.getShard(),
                            unmatchedOrder.getYearMonthDate(),
                            unmatchedOrder.getOrderId()
                    )
                    .build();
        } else {
            // quantity > 0 이면 UPDATE
            unmatchedOrderStatement = SimpleStatement.builder(
                            "UPDATE unmatched_order SET quantity = ? WHERE WHERE user_id = ? AND shard = ? AND year_month_date = ? AND order_id = ?")
                    .addPositionalValues(
                            unmatchedOrder.getQuantity(),
                            unmatchedOrder.getUserId(),
                            unmatchedOrder.getShard(),
                            unmatchedOrder.getYearMonthDate(),
                            unmatchedOrder.getOrderId()
                    )
                    .build();
        }

        // 3. LOGGED BATCH로 묶기
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(insertMatchedOrderStatement);
        batch.addStatement(unmatchedOrderStatement);

        // 4. 실행
        cassandraTemplate.getCqlOperations().execute(batch.build());
    }

    @Override
    public void deleteAll() {
        matchedOrderStoreRepository.deleteAll();
    }
}
