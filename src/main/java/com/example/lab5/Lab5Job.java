package com.example.lab5;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.LoggerFactory;

import static com.example.lab5.TransactionType.BILATERAL;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class Lab5Job {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(5_000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
		env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);

		env
				.addSource(new SourceFunction<TransactionEvent>() {
					@Override public void run(SourceContext<TransactionEvent> ctx) throws InterruptedException {
						ctx.collect(new TransactionEvent("1", 1, BILATERAL));
						ctx.collect(new TransactionEvent("1", 2, BILATERAL));
						ctx.collect(new TransactionEvent("2", 1, BILATERAL));
						ctx.collect(new TransactionEvent("2", 2, BILATERAL));
						ctx.collect(new TransactionEvent("3", 1, BILATERAL));
						Thread.currentThread().join();
					}

					@Override public void cancel() {
					}
				})
				.keyBy(TransactionEvent::getTransactionId)
				.map(new RichMapFunction<TransactionEvent, Report>() {
					private transient ValueState<Boolean> positionOpenState;

					@Override public void open(Configuration parameters) {
						positionOpenState = getRuntimeContext()
								.getState(new ValueStateDescriptor<>("positionOpen", Boolean.class));
					}

					@Override public Report map(TransactionEvent transactionEvent) throws Exception {
						Boolean positionOpen = positionOpenState.value();
						if (positionOpen == null) positionOpen = false;
						ReportType reportType;
						if (positionOpen) {
							reportType = ReportType.AMEND;
						} else {
							reportType = ReportType.NEW;
							positionOpenState.update(true);
						}
						return new Report(
								transactionEvent.getTransactionId(),
								transactionEvent.getTransactionType(),
								transactionEvent.getTransactionId() + "-" + transactionEvent.getTransactionVersion(),
								reportType);
					}
				})
				.addSink(new SinkFunction<Report>() {
					@Override public void invoke(Report report, Context context) {
						LoggerFactory.getLogger(getClass()).info("{}", report);
					}
				});

		env.execute("lab1");
	}
}
