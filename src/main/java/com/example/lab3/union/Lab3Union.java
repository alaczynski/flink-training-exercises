package com.example.lab3.union;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Random;

import static java.lang.Thread.sleep;

public class Lab3Union {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SingleOutputStreamOperator<Event> transactionEventStream = env.addSource(transactionEventSourceFunction(5));
        SingleOutputStreamOperator<Event> confirmationStream = env.addSource(confirmationSourceFunction(5));

        transactionEventStream
                .union(confirmationStream)
                .keyBy((KeySelector<Event, String>) Event::getId)
                .process(new KeyedProcessFunction<String, Event, Report>() {
                    private transient ValueState<TransactionEvent> transactionEventState;
                    private transient ValueState<ConfirmationEvent> confirmationEventState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<TransactionEvent> reportStateDescriptor = new ValueStateDescriptor<>("transactionEventState", TransactionEvent.class);
                        ValueStateDescriptor<ConfirmationEvent> responseStateDescriptor = new ValueStateDescriptor<>("confirmationEventState", ConfirmationEvent.class);

                        transactionEventState = getRuntimeContext().getState(reportStateDescriptor);
                        confirmationEventState = getRuntimeContext().getState(responseStateDescriptor);
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<Report> out) throws IOException {
                        if (value instanceof TransactionEvent) {
                            transactionEventState.update((TransactionEvent) value);
                        } else if (value instanceof ConfirmationEvent) {
                            confirmationEventState.update((ConfirmationEvent) value);
                        }

                        TransactionEvent transactionEvent = transactionEventState.value();
                        ConfirmationEvent confirmationEvent = confirmationEventState.value();

                        if (transactionEvent != null) {
                            out.collect(((confirmationEvent != null))
                                    ? Report.confirmedReport(transactionEvent.getTransactionId())
                                    : Report.notConfirmedReport(transactionEvent.getTransactionId())
                            );
                        }
                    }
                })
                .addSink(new SinkFunction<Report>() {
                    @Override
                    public void invoke(Report value, Context context) {
                        System.out.println(value.toString());
                    }
                });

        env.execute("lab3");
    }

    private static SourceFunction<Event> transactionEventSourceFunction(int amount) {
        return new SourceFunction<Event>() {
            private boolean running;

            @Override
            public void run(SourceContext<Event> ctx) throws InterruptedException {
                running = true;
                for (int i = 0; i < amount; i++) {
//                    sleep(new Random().nextInt(500));
                    ctx.collect(new TransactionEvent(String.valueOf(i)));
                    System.out.println("sent report: " + i);
                    if (!running) break;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }

    private static SourceFunction<Event> confirmationSourceFunction(int amount) {
        return new SourceFunction<Event>() {
            private boolean running;

            @Override
            public void run(SourceContext<Event> ctx) throws InterruptedException {
                running = true;
                for (int i = 0; i < amount; i++) {
//                    sleep(new Random().nextInt(500));
                    ctx.collect(new ConfirmationEvent(String.valueOf(i)));
                    System.out.println("sent response: " + i);
                    if (!running) break;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }
}
