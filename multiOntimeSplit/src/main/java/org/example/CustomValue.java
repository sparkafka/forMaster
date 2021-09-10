package org.example;

public class CustomValue {
    String window;
    int value;
    //event-time processing aka “producer time” is the default. This represents the time when the Kafka producer sent the original message.
    //ingestion-time processing aka “broker time” is the time when the Kafka broker received the original message.
    Long eventTime; // 8 bytes
    Long endTime;

    public CustomValue(String inputKey, int inputValue) {
        this.window = inputKey;
        this.value = inputValue;
        this.eventTime = 0L;
        this.endTime = 0L;
    }

    public String window() {
        return this.window;
    }

    public int value() {
        return this.value;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public void setEndTime() {
        this.endTime = System.currentTimeMillis();
    }
}
