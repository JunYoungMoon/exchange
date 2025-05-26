package com.exchange.order_completed.application.service;

import com.exchange.order_completed.application.command.ChartCommand;
import com.exchange.order_completed.application.command.CreateMatchedOrderStoreCommand;
import com.exchange.order_completed.application.command.CreateUnmatchedOrderStoreCommand;
import com.exchange.order_completed.common.exception.DuplicateUnmatchedOrderInformationException;
import com.exchange.order_completed.common.response.ResponseDto;
import com.exchange.order_completed.domain.cassandra.entity.ColdDataOrders;
import com.exchange.order_completed.domain.cassandra.entity.MatchedOrder;
import com.exchange.order_completed.domain.cassandra.entity.OrderType;
import com.exchange.order_completed.domain.cassandra.entity.UnmatchedOrder;
import com.exchange.order_completed.domain.cassandra.repository.MatchedOrderReader;
import com.exchange.order_completed.domain.cassandra.repository.MatchedOrderStore;
import com.exchange.order_completed.domain.cassandra.repository.UnmatchedOrderReader;
import com.exchange.order_completed.domain.cassandra.repository.UnmatchedOrderStore;
import com.exchange.order_completed.domain.postgres.entity.Chart;
import com.exchange.order_completed.infrastructure.enums.OperationType;
import com.exchange.order_completed.infrastructure.postgres.repository.ChartRepositoryStore;
import com.exchange.order_completed.presentation.dto.PagedResult;
import com.exchange.order_completed.presentation.dto.TradeDataResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCompletedService {

    private final MatchedOrderStore matchedOrderStore;
    private final MatchedOrderReader matchedOrderReader;
    private final UnmatchedOrderReader unmatchedOrderReader;
    private final UnmatchedOrderStore unmatchedOrderStore;
    private final ChartRepositoryStore chartRepositoryStore;

    //서비스 시작 날짜(조회 시 사용)
    private static final LocalDate SYSTEM_MIN_DATE = LocalDate.of(2020, 1, 1);

    // 날짜 범위를 나타내는 내부 클래스
    private static class DateRange {
        final LocalDate from;
        final LocalDate to;

        DateRange(LocalDate from, LocalDate to) {
            this.from = from;
            this.to = to;
        }
    }

    public Mono<Void> completeMatchedOrder(List<CreateMatchedOrderStoreCommand> commandList) {
        List<MatchedOrder> matchedOrderList = commandList.stream()
                .map(CreateMatchedOrderStoreCommand::toEntity)
                .toList();
        return matchedOrderStore.saveBatch(matchedOrderList);
    }

    public void updateUnmatchedOrderQuantity(MatchedOrder matchedOrder, UnmatchedOrder unmatchedOrder) {
        BigDecimal matchedOrderQuantity = matchedOrder.getQuantity();
        BigDecimal unmatchedOrderQuantity = unmatchedOrder.getQuantity();

        // 체결된 주문의 수량이 미체결 주문의 수량보다 적은 경우
        if (matchedOrderQuantity.compareTo(unmatchedOrderQuantity) < 0) {
            // 체결된 주문의 수량을 미체결 주문의 수량에서 빼줌
            unmatchedOrder.setQuantity(unmatchedOrderQuantity.subtract(matchedOrderQuantity));

            // 체결된 주문의 수량이 미체결 주문의 수량과 같은 경우
        } else if (matchedOrderQuantity.compareTo(unmatchedOrderQuantity) == 0) {
            // 미체결 주문의 수량을 0으로 설정
            unmatchedOrder.setQuantity(BigDecimal.ZERO);
        }
    }

    public void completeUnmatchedOrder(CreateUnmatchedOrderStoreCommand command) {
        UnmatchedOrder unmatchedOrder = command.unmatchedOrderToEntity();
        ColdDataOrders coldDataOrders = command.coldDataOrdersToEntity();

        if (command.operationType().equals(OperationType.DELETE)) {
            unmatchedOrderStore.deleteUnmatchedOrderAndColdDataOrders(unmatchedOrder, coldDataOrders);
        } else {
            unmatchedOrderStore.saveUnmatchedOrderAndColdDataOrders(unmatchedOrder, coldDataOrders);
        }
    }

    @Transactional
    public void saveChart(ChartCommand command) {
        Chart chart = Chart.from(command);
        chartRepositoryStore.save(chart);
    }

    public Mono<ResponseDto<PagedResult<TradeDataResponse>>> findMatchedOrderHistory(
            UUID userId,
            Instant cursor,
            int size,
            OrderType orderType,
            LocalDate startDate,
            LocalDate endDate
    ) {
        // 날짜 범위 계산
        DateRange range = calculateDateRange(startDate, endDate);

        // 샤드 리스트 준비
        List<Integer> shards = getShardList();

        // Repository에서 한 번에 범위 조회
        return matchedOrderReader
                .findByUserIdAndShardInAndYearMonthDateRange(
                        userId,
                        shards.get(0), shards.get(1), shards.get(2),
                        range.from, range.to
                ) // Flux<MatchedOrder>
                .filter(order -> orderType == null || orderType.equals(order.getOrderType()))
                // Flux를 List로 수집
                .collectList()
                .map(allOrders -> toPagedResult(allOrders, cursor, size, TradeDataResponse::fromMatchedEntity))
                .map(ResponseDto::success);
    }

    public Mono<PagedResult<TradeDataResponse>> findUnmatchedOrderHistory(
            UUID userId,
            Instant cursor,
            int size,
            OrderType orderType,
            LocalDate startDate,
            LocalDate endDate,
            String orderState
    ) {
        // 날짜 범위 계산
        DateRange range = calculateDateRange(startDate, endDate);

        // 샤드 리스트 준비
        List<Integer> shards = getShardList();

        // Repository에서 한 번에 범위 조회
        return unmatchedOrderReader
                .findByUserIdAndShardInAndYearMonthDateRange(
                        userId,
                        shards.get(0), shards.get(1), shards.get(2),
                        range.from, range.to
                ) // Flux<UnmatchedOrder>
                .filter(order -> orderType == null || orderType.equals(order.getOrderType()))
                .filter(order -> {
                    if (orderState == null || order.getOrderState() == null) return true;
                    return orderState.equalsIgnoreCase(order.getOrderState().name());
                })
                .collectList() // Mono<List<UnmatchedOrder>>
                .map(filteredOrders -> toPagedResult(
                        filteredOrders,
                        cursor,
                        size,
                        TradeDataResponse::fromUnmatchedEntity
                ));
    }

    //조회 공통 메서드 분리
    //날짜 범위 계산

    /**
     * 날짜 범위 계산 공통 메서드
     * - startDate, endDate 입력값에 따라 실제 조회 시작일/종료일을 계산
     * 1) 둘 다 null: 오늘 하루만
     * 2) startDate만: startDate~최대 3개월 후(또는 오늘)
     * 3) endDate만: endDate 기준 3개월 전~endDate (최소 SYSTEM_MIN_DATE 보장)
     * 4) 둘 다 있으면: 해당 구간 전체
     */
    private DateRange calculateDateRange(LocalDate startDate, LocalDate endDate) {
        LocalDate today = LocalDate.now();
        LocalDate fromDate;
        LocalDate toDate;

        if (startDate == null && endDate == null) {
            // 1) 시작일과 종료일이 모두 없는 경우: 오늘 하루만 조회
            fromDate = today;
            toDate = today;
        } else if (startDate != null && endDate == null) {
            // 2) 시작일만 있는 경우: 시작일부터 최대 3개월 조회
            fromDate = startDate;
            // 시작일 + 3개월이 오늘을 넘으면 오늘까지, 아니면 시작일 + 3개월까지
            toDate = startDate.plusMonths(3).isAfter(today) ? today : startDate.plusMonths(3);
        } else if (startDate == null && endDate != null) {
            // 3) 종료일만 있는 경우: 종료일 기준 3개월 전부터 종료일까지 조회
            toDate = endDate;
            // 종료일 - 3개월이 시스템 최소 날짜보다 이전이면 최소 날짜부터, 아니면 종료일 - 3개월부터
            fromDate = endDate.minusMonths(3).isBefore(SYSTEM_MIN_DATE)
                    ? SYSTEM_MIN_DATE
                    : endDate.minusMonths(3);
        } else {
            // 4) 시작일과 종료일이 모두 있는 경우: 해당 구간 전체 조회
            fromDate = startDate;
            toDate = endDate;
        }
        return new DateRange(fromDate, toDate);
    }

    /**
     * 샤드 리스트 반환 (현재 1,2,3)
     * - 샤드가 늘어나면 여기서 수정
     */
    private List<Integer> getShardList() {
        return Arrays.asList(1, 2, 3);
    }

    /**
     * 페이징/커서 처리 공통 메서드
     *
     * @param allOrders    전체 조회 결과
     * @param cursor       커서(마지막 createdAt)
     * @param size         페이지 크기
     * @param entityMapper 엔티티 -> 응답 DTO 변환 함수
     * @return PagedResult (응답 리스트, 다음 커서, hasNext)
     */
    private <T, R> PagedResult<R> toPagedResult(
            List<T> allOrders,
            Instant cursor,
            int size,
            Function<T, R> entityMapper
    ) {
        // 커서/정렬/페이징 처리
        List<T> filtered = allOrders.stream()
                .filter(order -> {
                    // 커서가 없거나, createdAt이 커서 이전인 데이터만
                    if (cursor == null) return true;
                    Instant createdAt;
                    if (order instanceof MatchedOrder mo) {
                        createdAt = mo.getCreatedAt();
                    } else if (order instanceof UnmatchedOrder uo) {
                        createdAt = uo.getCreatedAt();
                    } else {
                        throw new IllegalArgumentException("지원하지 않는 타입");
                    }
                    return createdAt.isBefore(cursor);
                })
                .limit(size + 1) // 다음 페이지 존재 여부 확인을 위해 size+1개 조회
                .toList();

        boolean hasNext = filtered.size() > size;
        List<T> pageData = hasNext ? filtered.subList(0, size) : filtered;

        // 엔티티 -> 응답 DTO 변환
        List<R> responseList = pageData.stream()
                .map(entityMapper)
                .toList();

        // 다음 커서 계산
        Instant nextCursor = pageData.isEmpty() ? null :
                (pageData.get(pageData.size() - 1) instanceof MatchedOrder mo
                        ? mo.getCreatedAt()
                        : ((UnmatchedOrder) pageData.get(pageData.size() - 1)).getCreatedAt());

        return new PagedResult<>(
                responseList,
                nextCursor != null ? LocalDateTime.ofInstant(nextCursor, ZoneId.systemDefault()) : null,
                hasNext
        );
    }
}
