package com.exchange.matching.common.exception;

public class RedisBackpressureException extends RuntimeException {
    public RedisBackpressureException(String message) {
        super(message);
    }
}
