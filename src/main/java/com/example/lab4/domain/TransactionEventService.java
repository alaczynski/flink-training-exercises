package com.example.lab4.domain;

public class TransactionEventService {
    public Report process(TransactionEvent transactionEvent, Position position) {
        boolean positionOpen = position.isOpen();
        ReportType reportType;
        if (positionOpen) {
            reportType = ReportType.AMEND;
        } else {
            reportType = ReportType.NEW;
            position.open();
        }
        return new Report(
                transactionEvent.getTransactionId(),
                transactionEvent.getTransactionType(),
                transactionEvent.getTransactionId() + "-" + transactionEvent.getTransactionVersion(),
                reportType);
    }
}
