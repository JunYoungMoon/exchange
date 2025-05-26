package com.exchange.order_completed.presentation.external;

import com.exchange.order_completed.application.service.OrderCompletedService;
import com.exchange.order_completed.common.UserInfoHeader;
import com.exchange.order_completed.common.response.ResponseDto;
import com.exchange.order_completed.domain.cassandra.entity.OrderState;
import com.exchange.order_completed.domain.cassandra.entity.OrderType;
import com.exchange.order_completed.presentation.dto.PagedResult;
import com.exchange.order_completed.presentation.dto.TradeDataResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

@RestController
@RequestMapping("/api/order_completed/histories")
@RequiredArgsConstructor
public class OrderHistoryController {

    private final OrderCompletedService completedService;

    @GetMapping("/matched-orders")
    public Mono<ResponseEntity<ResponseDto<PagedResult<TradeDataResponse>>>> findMatchedOrderHistory(
            ServerHttpRequest request,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime cursor,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "BUY") OrderType orderType,
            @RequestParam(required = false) LocalDate startDate,
            @RequestParam(required = false) LocalDate endDate
    ) {
        UserInfoHeader userInfo = new UserInfoHeader(request);

        Instant cursorInstant = cursor != null ? cursor.atZone(ZoneId.systemDefault()).toInstant() : null;

        return completedService
                .findMatchedOrderHistory(userInfo.getUserId(), cursorInstant, size, orderType, startDate, endDate)
                .map(ResponseEntity::ok);
    }

    @GetMapping("/unmatched-orders")
    public Mono<ResponseEntity<ResponseDto<PagedResult<TradeDataResponse>>>> findUnmatchedOrderHistory(
            ServerHttpRequest request,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime cursor,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "BUY", required = false) OrderType orderType,
            @RequestParam(required = false) LocalDate startDate,
            @RequestParam(required = false) LocalDate endDate,
            @RequestParam(defaultValue = "PENDING", required = false) OrderState orderState
    ) {
        UserInfoHeader userInfo = new UserInfoHeader(request);

        Instant cursorInstant = cursor != null
                ? cursor.atZone(ZoneId.systemDefault()).toInstant()
                : null;

        return completedService.findUnmatchedOrderHistory(
                        userInfo.getUserId(),
                        cursorInstant,
                        size,
                        orderType,
                        startDate,
                        endDate,
                        orderState.name()
                )
                .map(ResponseDto::success)
                .map(ResponseEntity::ok);
    }
}