package org.example;

public class CustomValue {
    int window;
    int value;
    int nodeNum; // Processed node number
    //event-time processing aka “producer time” is the default. This represents the time when the Kafka producer sent the original message.
    //ingestion-time processing aka “broker time” is the time when the Kafka broker received the original message.
    Long eventTime; // 8 bytes
    Long endTime;

    public CustomValue(int inputWindow, int inputValue) {
        this.window = inputWindow;
        this.value = inputValue;
        this.nodeNum = 0;
        this.eventTime = 0L;
        this.endTime = 0L;
    }

    public CustomValue(int inputWindow, int inputValue, Long inEventTime, Long inEndTime) {
        this.window = inputWindow;
        this.value = inputValue;
        this.nodeNum = 0;
        this.eventTime = inEventTime;
        this.endTime = inEndTime;
    }

    public CustomValue(int inputWindow, int inputValue, int inNodeNum, Long inEventTime, Long inEndTime) {
        this.window = inputWindow;
        this.value = inputValue;
        this.nodeNum = inNodeNum;
        this.eventTime = inEventTime;
        this.endTime = inEndTime;
    }

    public int window() {
        return this.window;
    }

    public int value() {
        return this.value;
    }

    public void setNodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
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
