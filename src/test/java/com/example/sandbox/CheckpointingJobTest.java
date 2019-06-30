package com.example.sandbox;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.apache.flink.configuration.ConfigConstants.LOCAL_START_WEBSERVER;

@Ignore
public class CheckpointingJobTest {
	private static final Configuration FLINK_CONFIG;

	static {
		FLINK_CONFIG = new Configuration();
		FLINK_CONFIG.setBoolean(LOCAL_START_WEBSERVER, true);
		FLINK_CONFIG.setString("state.backend", "filesystem");
		FLINK_CONFIG.setString("state.checkpoints.dir", new File("target/state/checkpoints").toURI().toString());
	}

	@ClassRule
	public static MiniClusterWithClientResource flink =
			new MiniClusterWithClientResource(
					new MiniClusterResourceConfiguration.Builder()
							.setConfiguration(FLINK_CONFIG)
							.setNumberSlotsPerTaskManager(2)
							.setNumberTaskManagers(1)
							.build());

	@Test
	public void run() throws Exception {
		System.out.println(flink.getRestAddres());
		// run job
		PackagedProgram program = new PackagedProgram(CheckpointingJob.class);
		Executors.newSingleThreadExecutor().submit((Callable<Void>) () -> {
			flink.getClusterClient().run(program, 1);
			return null;
		});
		Thread.sleep(5_000);

		// cancel with savepoint
		JobStatusMessage job = flink.getClusterClient().listJobs().get().iterator().next();
		String savepoint = flink.getClusterClient().cancelWithSavepoint(job.getJobId(), new File("target/state/savepoints").toURI().toString());
		System.out.println(savepoint);

		// block
		Thread.currentThread().join();
	}
}