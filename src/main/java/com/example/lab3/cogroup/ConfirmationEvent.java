package com.example.lab3.cogroup;

public class ConfirmationEvent {
    private final String transactionId;
    private final long timestamp;

    public ConfirmationEvent(String transactionId, long timestamp) {
        this.transactionId = transactionId;
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ConfirmationEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
