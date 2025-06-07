package com.exchange.matching.infrastructure.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Builder
@Data
public class DLQMessage {
    private KafkaMatchingEvent originalMessage;
    private String originalTopic;
    private int originalPartition;
    private long originalOffset;
    private String originalKey;
    private int totalRetryCount;
    private String finalFailureReason;
    private Instant dlqTimestamp;
}