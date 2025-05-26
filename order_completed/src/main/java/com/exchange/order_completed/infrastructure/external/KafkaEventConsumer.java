package com.exchange.order_completed.infrastructure.external;

import com.exchange.order_completed.application.command.ChartCommand;
import com.exchange.order_completed.application.command.CreateMatchedOrderStoreCommand;
import com.exchange.order_completed.application.command.CreateUnmatchedOrderStoreCommand;
import com.exchange.order_completed.application.service.OrderCompletedService;
import com.exchange.order_completed.infrastructure.dto.CompletedOrderChangeEvent;
import com.exchange.order_completed.infrastructure.dto.KafkaMatchedOrderStoreEvent;
import com.exchange.order_completed.infrastructure.dto.KafkaUnmatchedOrderStoreEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventConsumer {

    private final KafkaReceiver<String, KafkaMatchedOrderStoreEvent> kafkaReceiver;
    private final OrderCompletedService orderCompletedService;

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaListeners() {
        kafkaReceiver.receive()
                .publishOn(Schedulers.boundedElastic())
                .bufferTimeout(3000, Duration.ofMillis(500))
                .flatMap(this::consumeMatchedMessage)
                .onErrorContinue((err, obj) -> log.error("Kafka 처리 중 오류", err))
                .subscribe();
    }

    private Mono<Void> consumeMatchedMessage(List<ReceiverRecord<String, KafkaMatchedOrderStoreEvent>> recordList) {
        log.info("Kafka 메시지 수신: {}", recordList.size());

        List<CreateMatchedOrderStoreCommand> commandList = extractCommandList(recordList);

        return orderCompletedService.completeMatchedOrder(commandList)
                .then(Mono.fromRunnable(() ->
                        recordList.forEach(record -> record.receiverOffset().acknowledge())));
    }

    private List<CreateMatchedOrderStoreCommand> extractCommandList(List<ReceiverRecord<String, KafkaMatchedOrderStoreEvent>> recordList) {
        return recordList.stream()
                .flatMap(record -> {
                    KafkaMatchedOrderStoreEvent event = record.value();
                    return Stream.of(
                            CreateMatchedOrderStoreCommand.fromBuyOrderInfo(event),
                            CreateMatchedOrderStoreCommand.fromSellOrderInfo(event)
                    );
                })
                .toList();
    }

    @KafkaListener(
            topics = {"matching-to-order_completed.execute-order-unmatched"},
            containerFactory = "unmatchedOrderKafkaListenerContainerFactory")
    public void consumeUnmatchedMessage(List<ConsumerRecord<String, KafkaUnmatchedOrderStoreEvent>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, KafkaUnmatchedOrderStoreEvent> record : records) {
            KafkaUnmatchedOrderStoreEvent value = record.value();
            CreateUnmatchedOrderStoreCommand command = CreateUnmatchedOrderStoreCommand.from(value);

            orderCompletedService.completeUnmatchedOrder(command);
        }

        ack.acknowledge();
    }

    @KafkaListener(
            topics = "cassandra.exchange.completed_order",
            containerFactory = "chartKafkaListenerContainerFactory")
    public void savaChart(CompletedOrderChangeEvent record) {

        if (!"i".equals(record.getOp())) {
            return ;
        }

        ChartCommand command = ChartCommand.fromEvent(record);

        orderCompletedService.saveChart(command);
    }
}
