package com.exchange.order_completed.application.service;

import com.exchange.order_completed.application.TimeInterval;
import com.exchange.order_completed.domain.postgres.entity.TradeDataInfo;
import reactor.core.publisher.Flux;

public interface TradeService {

    Flux<TradeDataInfo> getTradeInfo(String pair, TimeInterval time);
}
