package com.exchange.order_completed.domain.cassandra.repository;

import com.exchange.order_completed.domain.cassandra.entity.ColdDataOrders;
import com.exchange.order_completed.domain.cassandra.entity.UnmatchedOrder;

public interface UnmatchedOrderStore {

    void save(UnmatchedOrder unmatchedOrder);
    void delete(UnmatchedOrder unmatchedOrder);
    void saveUnmatchedOrderAndColdDataOrders(UnmatchedOrder unmatchedOrder, ColdDataOrders coldDataOrders);
    void deleteUnmatchedOrderAndColdDataOrders(UnmatchedOrder unmatchedOrder, ColdDataOrders coldDataOrders);
}
