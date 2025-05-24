package com.exchange.matching.domain.service;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import reactor.core.publisher.Mono;

public interface MatchingService {
    void matchOrders(CreateMatchingCommand command);
    MatchingVersion getVersion();

    default Mono<Void> matchOrdersReactive(CreateMatchingCommand command) {
        return Mono.fromRunnable(() -> matchOrders(command));
    }
}
