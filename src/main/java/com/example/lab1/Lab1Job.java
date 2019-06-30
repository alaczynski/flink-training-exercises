package com.example.lab1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Lab1Job {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// webui
//		Configuration configuration = new Configuration();
//		configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment
//				.createLocalEnvironmentWithWebUI(configuration);

		env
				.addSource(new SourceFunction<TransactionEvent>() {
					private boolean running;

					@Override public void run(SourceContext<TransactionEvent> ctx) {
						running = true;
						for (int i = 0; i < 100 && running; i++) {
							TransactionEvent transactionEvent = new TransactionEvent(String.valueOf(i + 1), 0, TransactionType.values()[i % 3]);
							ctx.collect(transactionEvent);
						}
					}

					@Override public void cancel() {
						running = true;
					}
				})
				.filter(event -> event.getTransactionType() != TransactionType.BLOCK)
				.map(event -> new Report(event.getTransactionId(), event.getTransactionType(),
						event.getTransactionId() + event.getTransactionVersion(), ReportType.NEW))
				.print();

		env.execute("lab1");
	}
}
