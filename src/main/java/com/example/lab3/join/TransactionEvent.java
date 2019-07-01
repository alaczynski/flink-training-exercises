package com.example.lab3.join;

public class TransactionEvent {
    private final String transactionId;
    private final long timestamp;

    public TransactionEvent(String transactionId, long timestamp) {
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
        return "TransactionEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
