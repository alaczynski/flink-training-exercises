package com.example.sandbox;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class CheckpointingJob {

	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameters);

		// enable checkpointing, mode: AT_LEAST_ONCE, EXACTLY_ONCE
		env.enableCheckpointing(5_000, CheckpointingMode.EXACTLY_ONCE);
		// make sure 5s of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(60_000);
		// enable externalized checkpoints which are retained after job cancellation
		env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
		// fail/continue task on checkpoint errors, default true=fail task
		env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

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
				}).name("source").uid("source-id")
				.keyBy(value -> "all")
				.map(new RichMapFunction<Long, String>() {
					private transient ValueState<Long> counterState;

					@Override public void open(Configuration parameters) {
						counterState = getRuntimeContext().getState(new ValueStateDescriptor<>("counter", Long.class));
					}

					@Override public String map(Long value) throws IOException {
						Long counter = counterState.value();
						if (counter == null) counter = 0L;
						counterState.update(++counter);
						System.out.println("counter: " + counter);
						return value.toString();
					}
				}).name("map").uid("map")
				.print();

		env.execute();
	}
}
