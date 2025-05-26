package com.exchange.order_completed.presentation.external;

import com.exchange.order_completed.application.TimeInterval;
import com.exchange.order_completed.application.service.TradeService;
import com.exchange.order_completed.domain.postgres.entity.TradeDataInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/order_completed/charts")
@RequiredArgsConstructor
public class ChartController {

    private final TradeService tradeService;

    @GetMapping("/{pair}/{interval}")
    public Flux<TradeDataInfo> getChartData(@PathVariable String pair, @PathVariable TimeInterval interval) {
        return tradeService.getTradeInfo(pair, interval);
    }
}