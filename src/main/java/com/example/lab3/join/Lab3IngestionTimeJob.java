package com.example.lab3.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static java.lang.Thread.sleep;

public class Lab3IngestionTimeJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(10);

        DataStreamSource<TransactionEvent> reports = env.addSource(transactionEventSourceFunction(5));
        DataStreamSource<ConfirmationEvent> reportResponses = env.addSource(confirmationSourceFunction(5));

        reports
                .join(reportResponses)
                .where((KeySelector<TransactionEvent, String>) TransactionEvent::getTransactionId)
                .equalTo((KeySelector<ConfirmationEvent, String>) ConfirmationEvent::getTransactionId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
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
            public void run(SourceContext<TransactionEvent> ctx) throws InterruptedException {
                running = true;
                for (int i = 0; i < amount; i++) {
                    ctx.collect(new TransactionEvent(String.valueOf(i), i));
                    System.out.println("sent report: " + i);
                    sleep(500);
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
                    sleep(500);
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
