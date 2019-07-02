package com.example.lab3.join;

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

import static java.lang.Thread.sleep;

public class Lab3EventTimeJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<TransactionEvent> transactionEvents = env.addSource(transactionEventSourceFunction(5))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TransactionEvent>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(TransactionEvent element) {
                        return element.getTimestamp();
                    }
                });
        SingleOutputStreamOperator<ConfirmationEvent> confirmationEvents = env.addSource(confirmationSourceFunction(5))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ConfirmationEvent>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(ConfirmationEvent element) {
                        return element.getTimestamp();
                    }
                });

        transactionEvents
                .join(confirmationEvents)
                .where((KeySelector<TransactionEvent, String>) TransactionEvent::getTransactionId)
                .equalTo((KeySelector<ConfirmationEvent, String>) ConfirmationEvent::getTransactionId)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .allowedLateness(Time.minutes(1))
                .apply(new JoinFunction<TransactionEvent, ConfirmationEvent, Report>() {
                           @Override
                           public Report join(TransactionEvent transactionEvent, ConfirmationEvent confirmationEvent) throws Exception {
                               if (transactionEvent != null) {
                                   return (confirmationEvent != null) ?
                                           Report.confirmedReport(transactionEvent.getTransactionId()) :
                                           Report.notConfirmedReport(transactionEvent.getTransactionId());
                               }
                               return null;
                           }
                       }
                )
                .addSink(new SinkFunction<Report>() {
                    @Override
                    public void invoke(Report value, Context context) {
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

    private static SourceFunction<ConfirmationEvent> confirmationSourceFunction(int amount) {
        return new SourceFunction<ConfirmationEvent>() {
            private boolean running;

            @Override
            public void run(SourceContext<ConfirmationEvent> ctx) throws InterruptedException {
                running = true;
                for (int i = 0; i < amount; i++) {
                    ctx.collect(new ConfirmationEvent(String.valueOf(i), i));
                    System.out.println("sent response: " + i);
                    sleep(200);
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
