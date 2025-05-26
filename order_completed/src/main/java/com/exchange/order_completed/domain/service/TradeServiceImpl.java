package com.exchange.order_completed.domain.service;

import com.exchange.order_completed.application.TimeInterval;
import com.exchange.order_completed.application.service.TradeService;
import com.exchange.order_completed.domain.postgres.entity.TradeDataInfo;
import com.exchange.order_completed.infrastructure.postgres.repository.ChartRepositoryReader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class TradeServiceImpl implements TradeService {

    private final ChartRepositoryReader chartRepositoryReader;

    public Flux<TradeDataInfo> getTradeInfo(String pair, TimeInterval timeInterval) {
        String formattedPair = pair.toLowerCase().replace("/", "");
        String timeFormat = timeInterval.getShortCode();
        String viewName = String.format("%s_%s_trades", formattedPair, timeFormat);

        return chartRepositoryReader.searchDataFromView(viewName, timeInterval.getInterval());
    }

    private boolean isValidIdentifier(String identifier) {
        // SQL 식별자에 대한 간단한 검증 로직
        // 알파벳, 숫자, 언더스코어만 허용하고 SQL 키워드 금지 등
        return identifier != null && identifier.matches("^[a-zA-Z0-9]+$");
    }
}
