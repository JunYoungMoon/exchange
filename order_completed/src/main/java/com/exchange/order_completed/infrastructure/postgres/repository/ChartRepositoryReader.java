package com.exchange.order_completed.infrastructure.postgres.repository;

import com.exchange.order_completed.domain.postgres.entity.TradeDataInfo;
import reactor.core.publisher.Flux;

public interface ChartRepositoryReader {

    Flux<TradeDataInfo> searchDataFromView(String viewName, String timeColumnName);
}
