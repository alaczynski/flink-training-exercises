package com.example.lab2;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static com.example.lab2.TransactionType.BILATERAL;

public class Lab2Job {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
				.addSource(new SourceFunction<TransactionEvent>() {
					@Override public void run(SourceContext<TransactionEvent> ctx) {
						ctx.collect(new TransactionEvent("1", 1, BILATERAL));
						ctx.collect(new TransactionEvent("1", 2, BILATERAL));
						ctx.collect(new TransactionEvent("2", 1, BILATERAL));
						ctx.collect(new TransactionEvent("2", 2, BILATERAL));
						ctx.collect(new TransactionEvent("3", 1, BILATERAL));
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
				.print();

		env.execute("lab1");
	}
}
