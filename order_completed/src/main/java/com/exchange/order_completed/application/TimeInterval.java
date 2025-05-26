package com.exchange.order_completed.application;

public enum TimeInterval {

    ONE_MINUTE("m1","minute"),
    THREE_MINUTES("m3","minute"),
    FIVE_MINUTES("m5","minute"),
    FIFTEEN_MINUTES("m15","fifteen_minute"),
    THIRTY_MINUTES("m30","thirty_minute"),
    ONE_HOUR("h1","hour"),
    THREE_HOUR("h1","three_hour"),
    SIX_HOURS("h6","six_hour"),
    TWELVE_HOURS("h12","twelve_hour"),
    ONE_DAY("d1","day"),
    ONE_WEEK("w1","week"),
    ONE_MONTH("mon1","month"); // 예시

    private final String shortCode;
    private final String interval;


    TimeInterval(String shortCode, String interval) {
        this.shortCode = shortCode;
        this.interval = interval;
    }

    public String getShortCode() {
        return shortCode;
    }

    public String getInterval() {
        return interval;
    }

}