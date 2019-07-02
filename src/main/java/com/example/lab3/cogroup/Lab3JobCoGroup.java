package com.example.lab3.cogroup;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.setOut;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.stream.StreamSupport.stream;

public class Lab3JobCoGroup {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SingleOutputStreamOperator<TransactionEvent> transactionEventStream = env.addSource(transactionEventSourceFunction(5))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TransactionEvent>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(TransactionEvent element) {
                        return element.getTimestamp();
                    }
                });
        SingleOutputStreamOperator<ConfirmationEvent> confirmationEventStream = env.addSource(confirmationSourceFunction(5))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ConfirmationEvent>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(ConfirmationEvent element) {
                        return element.getTimestamp();
                    }
                });

        transactionEventStream
                .coGroup(confirmationEventStream)
                .where((KeySelector<TransactionEvent, String>) TransactionEvent::getTransactionId)
                .equalTo((KeySelector<ConfirmationEvent, String>) ConfirmationEvent::getTransactionId)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new CoGroupFunction<TransactionEvent, ConfirmationEvent, Report>() {
                    @Override
                    public void coGroup(Iterable<TransactionEvent> transactionEvents, Iterable<ConfirmationEvent> confirmationEvents, Collector<Report> out) throws Exception {
                        List<String> confirmedTransactionIds = stream(confirmationEvents.spliterator(), false)
                                .map(ConfirmationEvent::getTransactionId)
                                .collect(Collectors.toList());

                        List<String> transactionIds = stream(transactionEvents.spliterator(), false)
                                .map(TransactionEvent::getTransactionId)
                                .collect(Collectors.toList());

                        System.out.println("================");
                        System.out.println("confirmed ids: " + confirmedTransactionIds);
                        System.out.println("transaction ids: " + transactionIds);
                        System.out.println("================");

                        transactionIds.stream().forEach(
                                new Consumer<String>() {
                                    @Override
                                    public void accept(String transactionId) {
                                        out.collect(confirmedTransactionIds.contains(transactionId) ?
                                                Report.confirmedReport(transactionId) :
                                                Report.notConfirmedReport(transactionId)
                                        );
                                }
                    }
                        );
                }
    })
            .

    addSink(new SinkFunction<Report>() {
        @Override
        public void invoke (Report value, Context context){
            System.out.println(value.toString());
        }
    });

        env.execute("lab3");
}

    private static SourceFunction<TransactionEvent> transactionEventSourceFunction(int amount) {
        return new SourceFunction<TransactionEvent>() {
            private boolean running;

            @Override
            public void run(SourceContext<TransactionEvent> ctx) {
                running = true;
                for (int i = 0; i < amount; i++) {
                    ctx.collect(new TransactionEvent(String.valueOf(i), i));
                    System.out.println("sent transaction: " + i);
                    if (!running) break;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }

    private static SourceFunction<ConfirmationEvent> confirmationSourceFunction(int amount) {
        return new SourceFunction<ConfirmationEvent>() {
            private boolean running;

            @Override
            public void run(SourceContext<ConfirmationEvent> ctx) throws InterruptedException {
                running = true;
                for (int i = 0; i < amount; i++) {
                    ctx.collect(new ConfirmationEvent(String.valueOf(i), i));
                    System.out.println("sent confirmation: " + i);
                    sleep(1000);
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
