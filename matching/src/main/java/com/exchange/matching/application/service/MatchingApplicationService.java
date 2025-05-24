package com.exchange.matching.application.service;

import com.exchange.matching.application.command.CreateMatchingCommand;
import com.exchange.matching.application.enums.MatchingVersion;
import com.exchange.matching.domain.service.MatchingServiceRegistry;
import com.exchange.matching.util.LockManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class MatchingApplicationService {
    private final MatchingServiceRegistry serviceRegistry;
    private final LockManager lockManager;

    public void processMatching(CreateMatchingCommand command, MatchingVersion version) {
        log.info("Processing matching for version: {}", version);

        if (version.requiresLock()) {
            String lockKey = generateLockKey(command);
            lockManager.executeWithLock(() -> executeMatching(command, version), lockKey);
        } else {
            executeMatching(command, version);
        }
    }

    public Mono<Void> processMatchingReactive(CreateMatchingCommand command, MatchingVersion version) {
        return executeMatchingReactive(command, version);
    }

    private Mono<Void> executeMatchingReactive(CreateMatchingCommand command, MatchingVersion version) {
        return serviceRegistry.getService(version).matchOrdersReactive(command);
    }

    private void executeMatching(CreateMatchingCommand command, MatchingVersion version) {
        serviceRegistry.getService(version).matchOrders(command);
    }

    private String generateLockKey(CreateMatchingCommand command) {
        return String.format("%s:%s:lock", command.tradingPair(), command.orderType());
    }
}