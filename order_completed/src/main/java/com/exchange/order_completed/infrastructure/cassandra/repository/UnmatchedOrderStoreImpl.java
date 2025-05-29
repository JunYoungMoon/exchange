package com.exchange.order_completed.infrastructure.cassandra.repository;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.exchange.order_completed.domain.cassandra.entity.ColdDataOrders;
import com.exchange.order_completed.domain.cassandra.entity.UnmatchedOrder;
import com.exchange.order_completed.domain.cassandra.repository.UnmatchedOrderStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class UnmatchedOrderStoreImpl implements UnmatchedOrderStore {

    private final UnmatchedOrderStoreRepository unmatchedOrderStoreRepository;
    private final CassandraTemplate cassandraTemplate;
    private final CqlTemplate cqlTemplate;

    @Override
    public void save(UnmatchedOrder unmatchedOrder) {
        unmatchedOrderStoreRepository.save(unmatchedOrder);
    }

    @Override
    public void delete(UnmatchedOrder unmatchedOrder) {
        unmatchedOrderStoreRepository.delete(unmatchedOrder);
    }

    @Override
    public void saveUnmatchedOrderAndColdDataOrders(UnmatchedOrder unmatchedOrder, ColdDataOrders coldDataOrders) {
        // 1. INSERT INTO unmatched_order
        SimpleStatement insertUnmatchedOrders = SimpleStatement.builder(
                        "INSERT INTO unmatched_order (user_id, shard, year_month_date, order_id, created_at, price, quantity, order_type, order_state, trading_pair) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .addPositionalValues(
                        unmatchedOrder.getUserId(),
                        unmatchedOrder.getShard(),
                        unmatchedOrder.getYearMonthDate(),
                        unmatchedOrder.getOrderId(),
                        unmatchedOrder.getCreatedAt(),
                        unmatchedOrder.getPrice(),
                        unmatchedOrder.getQuantity(),
                        unmatchedOrder.getOrderType().toString(),
                        unmatchedOrder.getOrderState().toString(),
                        unmatchedOrder.getTradingPair()
                )
                .build();

        // 2. INSERT INTO cold_data_orders
        SimpleStatement insertColdDataOrders = SimpleStatement.builder(
                        "INSERT INTO cold_data_orders (trading_pair, order_type, price_order, price, order_id, quantity, timestamp, user_id, order_state) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .addPositionalValues(
                        coldDataOrders.getTradingPair(),
                        coldDataOrders.getOrderType().toString(),
                        coldDataOrders.getPriceOrder(),
                        coldDataOrders.getPrice(),
                        coldDataOrders.getOrderId(),
                        coldDataOrders.getQuantity(),
                        coldDataOrders.getTimestamp(),
                        coldDataOrders.getUserId(),
                        coldDataOrders.getOrderState().toString()
                )
                .build();

        // 3. LOGGED BATCH로 묶기
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(insertUnmatchedOrders);
        batch.addStatement(insertColdDataOrders);

        // 4. 실행
        cassandraTemplate.getCqlOperations().execute(batch.build());
    }

    @Override
    public void deleteUnmatchedOrderAndColdDataOrders(UnmatchedOrder unmatchedOrder, ColdDataOrders coldDataOrders) {
        // 1. DELETE FROM unmatched_order
        SimpleStatement deleteUnmatchedOrder = SimpleStatement.builder(
                        "DELETE FROM unmatched_order " +
                                "WHERE user_id = ? AND shard = ? AND year_month_date = ? AND order_id = ?")
                .addPositionalValues(
                        unmatchedOrder.getUserId(),
                        unmatchedOrder.getShard(),
                        unmatchedOrder.getYearMonthDate(),
                        unmatchedOrder.getOrderId()
                )
                .build();

        // 2. DELETE FROM cold_data_orders
        SimpleStatement deleteColdDataOrder = SimpleStatement.builder(
                        "DELETE FROM cold_data_orders " +
                                "WHERE trading_pair = ? AND order_type = ? AND price_order = ? AND price = ? AND order_id = ?")
                .addPositionalValues(
                        coldDataOrders.getTradingPair(),
                        coldDataOrders.getOrderType().toString(),
                        coldDataOrders.getPriceOrder(),
                        coldDataOrders.getPrice(),
                        coldDataOrders.getOrderId()
                )
                .build();

        // 3. LOGGED BATCH로 묶기
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(deleteUnmatchedOrder);
        batch.addStatement(deleteColdDataOrder);

        // 4. 실행
        cassandraTemplate.getCqlOperations().execute(batch.build());
    }
}
