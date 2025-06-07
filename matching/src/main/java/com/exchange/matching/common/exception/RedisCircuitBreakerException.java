package com.exchange.matching.common.exception;

public class RedisCircuitBreakerException extends RuntimeException {
    public RedisCircuitBreakerException(String message) {
        super(message);
    }
}
