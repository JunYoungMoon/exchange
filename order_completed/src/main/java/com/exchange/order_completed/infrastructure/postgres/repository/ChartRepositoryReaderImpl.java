package com.exchange.order_completed.infrastructure.postgres.repository;

import com.exchange.order_completed.domain.postgres.entity.TradeDataInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@RequiredArgsConstructor
@Repository
public class ChartRepositoryReaderImpl implements ChartRepositoryReader{

    private final DatabaseClient databaseClient;

    public Flux<TradeDataInfo> searchDataFromView(String viewName, String timeColumnName) {
        String sql = "SELECT " + timeColumnName + ", pair, first_price, last_price, max_price, min_price, amount FROM " + viewName;

        return databaseClient.sql(sql)
                .map((row, metadata) -> {
                    TradeDataInfo tradeData = new TradeDataInfo();
                    tradeData.setMinute(row.get(timeColumnName, LocalDateTime.class));
                    tradeData.setPair(row.get("pair", String.class));
                    tradeData.setFirstPrice(row.get("first_price", BigDecimal.class));
                    tradeData.setLastPrice(row.get("last_price", BigDecimal.class));
                    tradeData.setMaxPrice(row.get("max_price", BigDecimal.class));
                    tradeData.setMinPrice(row.get("min_price", BigDecimal.class));
                    tradeData.setAmount(row.get("amount", BigDecimal.class));
                    return tradeData;
                })
                .all();
    }
}