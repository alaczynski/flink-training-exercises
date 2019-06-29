package com.example.sandbox;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MetricsJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 10);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.getConfig().setGlobalJobParameters(params);
		env
				.addSource(new SourceFunction<Long>() {
					private boolean running;

					@Override public void run(SourceContext<Long> ctx) throws Exception {
						running = true;
						long counter = 0;
						while (running) {
							ctx.collect(++counter);
							Thread.sleep(1000);
						}
					}

					@Override public void cancel() {
						running = false;
					}
				})
				.map(new RichMapFunction<Long, String>() {
					private transient String lastValue;

					@Override public void open(Configuration parameters) {
						getRuntimeContext().getMetricGroup().addGroup("my-group").gauge("my-gauge", () -> lastValue);
						getRuntimeContext().getMetricGroup().gauge("my-gauge", () -> lastValue);
					}

					@Override public String map(Long value) {
						lastValue = value.toString();
						return value.toString();
					}
				})
				.print();
		env.execute();
	}
}
