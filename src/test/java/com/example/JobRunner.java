package com.example;

import com.example.lab1.Lab1Job;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class JobRunner {
	private static final Configuration FLINK_CONFIG;

	static {
		FLINK_CONFIG = new Configuration();
		FLINK_CONFIG.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
	}

	@ClassRule
	public static MiniClusterWithClientResource flink =
			new MiniClusterWithClientResource(
					new MiniClusterResourceConfiguration.Builder()
							.setConfiguration(FLINK_CONFIG)
							.build());

	@Test
	public void run() throws Exception {
		System.out.println(flink.getRestAddres());
		Lab1Job.main(new String[]{});
	}
}
