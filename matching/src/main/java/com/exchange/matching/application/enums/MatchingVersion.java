package com.exchange.matching.application.enums;

import lombok.Getter;

public enum MatchingVersion {
    V1A("v1a", false),
    V1B("v1b", false),
    V2("v2", true),  // 분산 락이 필요한 버전
    V3("v3", false),
    V4("v4", false),
    V5("v5", false),
    V6A("v6a", false),
    V6B("v6b", false),
    V6C("v6c", false),
    V6D("v6d", false);

    @Getter
    private final String code;
    private final boolean requiresLock;

    MatchingVersion(String code, boolean requiresLock) {
        this.code = code;
        this.requiresLock = requiresLock;
    }

    public boolean requiresLock() { return requiresLock; }

    public static MatchingVersion fromCode(String code) {
        for (MatchingVersion version : values()) {
            if (version.code.equals(code)) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unknown version: " + code);
    }
}