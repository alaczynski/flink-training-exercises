package com.example.lab4.infra;

import com.example.lab4.app.TransactionEventHandler;
import com.example.lab4.domain.Report;
import com.example.lab4.domain.TransactionEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class TransactionEventRichFunction extends RichMapFunction<TransactionEvent, Report> {
    private transient TransactionEventHandler transactionEventHandler;

    @Override public void open(Configuration parameters) {
        transactionEventHandler = new TransactionEventHandler(new PositionFlinkRepository(getRuntimeContext()));
    }

    @Override public Report map(TransactionEvent transactionEvent) {
        return transactionEventHandler.handle(transactionEvent);
    }
}
