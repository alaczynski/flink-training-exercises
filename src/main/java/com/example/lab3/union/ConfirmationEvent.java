package com.example.lab3.union;

public class ConfirmationEvent extends Event {
    public ConfirmationEvent(String transactionId) {
        super(transactionId);
    }

    public String getTransactionId() {
        return getId();
    }

    @Override
    public String toString() {
        return "ConfirmationEvent{" +
                "transactionId='" + getTransactionId() + '\'' +
                '}';
    }
}
