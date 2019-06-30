package com.example.lab2;

public class TransactionEvent {
	private final String transactionId;
	private final long transactionVersion;
	private final TransactionType transactionType;

	public TransactionEvent(String transactionId, long transactionVersion, TransactionType transactionType) {
		this.transactionId = transactionId;
		this.transactionVersion = transactionVersion;
		this.transactionType = transactionType;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public long getTransactionVersion() {
		return transactionVersion;
	}

	public TransactionType getTransactionType() {
		return transactionType;
	}

	@Override public String toString() {
		return "TransactionEvent{" +
				"transactionId='" + transactionId + '\'' +
				", transactionVersion=" + transactionVersion +
				", transactionType=" + transactionType +
				'}';
	}
}
