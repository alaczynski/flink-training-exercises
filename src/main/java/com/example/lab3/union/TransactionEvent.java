package com.example.lab3.union;

public class TransactionEvent extends Event {
    public TransactionEvent(String transactionId) {
        super(transactionId);
    }

    public String getTransactionId() {
        return getId();
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "transactionId='" + getTransactionId() +
                '}';
    }
}
