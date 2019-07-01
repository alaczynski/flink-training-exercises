package com.example.topology;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class JobTopology {

	public static void main(String[] args) throws Exception {

		// webui
		Configuration configuration = new Configuration();
		configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironmentWithWebUI(configuration);

		env.setParallelism(3);
		env.disableOperatorChaining();

		env
				.addSource(new ParallelSourceFunction<TransactionEvent>() {
					private boolean running;

					@Override public void run(SourceContext<TransactionEvent> ctx) throws InterruptedException {
						running = true;
						for (int i = 0; i < 1000 && running; i++) {
							TransactionEvent transactionEvent = new TransactionEvent(String.valueOf(i + 1), 0, TransactionType.values()[i % 3]);
							ctx.collect(transactionEvent);
							Thread.sleep(1000);
						}
					}

					@Override public void cancel() {
						running = true;
					}
				})
				.filter(event -> event.getTransactionType() != TransactionType.BLOCK)
				.map(event -> new Report(event.getTransactionId(), event.getTransactionType(), event.getTransactionId() + "-" + event.getTransactionVersion(), ReportType.NEW))
				.print();

		env.execute("lab");
	}
}
