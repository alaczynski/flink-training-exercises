package com.example.lab4.infra;

import com.example.lab4.domain.TransactionEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static com.example.lab4.domain.TransactionType.BILATERAL;

public class TransactionEventSourceFunction implements SourceFunction<TransactionEvent> {
    @Override public void run(SourceContext<TransactionEvent> ctx) {
        ctx.collect(new TransactionEvent("1", 1, BILATERAL));
        ctx.collect(new TransactionEvent("1", 2, BILATERAL));
        ctx.collect(new TransactionEvent("2", 1, BILATERAL));
        ctx.collect(new TransactionEvent("2", 2, BILATERAL));
        ctx.collect(new TransactionEvent("3", 1, BILATERAL));
    }

    @Override public void cancel() {
    }
}
