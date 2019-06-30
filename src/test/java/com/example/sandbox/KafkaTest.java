package com.example.sandbox;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.metrics.prometheus.PrometheusReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Ignore
public class KafkaTest {
	private static final Configuration FLINK_CONFIG;

	static {
		FLINK_CONFIG = new Configuration();
		FLINK_CONFIG.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		FLINK_CONFIG.setString("metrics.reporters", "jmx,prometheus");
		FLINK_CONFIG.setString("metrics.reporter.jmx.class", JMXReporter.class.getName());
		FLINK_CONFIG.setString("metrics.reporter.prometheus.class", PrometheusReporter.class.getName());
	}

	@ClassRule
	public static EmbeddedKafkaRule kafka = new EmbeddedKafkaRule(1, true, "topic.input", "topic.output");
	@ClassRule
	public static MiniClusterWithClientResource flink =
			new MiniClusterWithClientResource(
					new MiniClusterResourceConfiguration.Builder()
							.setConfiguration(FLINK_CONFIG)
							.setNumberSlotsPerTaskManager(2)
							.setNumberTaskManagers(1)
							.build());

	@Test
	public void name() throws Exception {
		// start job
		Future<Void> job = Executors.newSingleThreadExecutor().submit(() -> {
			KafkaJob.main(new String[]{
					"--bootstrap.servers", kafka.getEmbeddedKafka().getBrokersAsString()
			});
			return null;
		});
		System.out.println(flink.getRestAddres());
		// send message
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", kafka.getEmbeddedKafka().getBrokersAsString());
		producerProperties.put("key.serializer", StringSerializer.class.getName());
		producerProperties.put("value.serializer", StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		RecordMetadata record = producer.send(new ProducerRecord<>("topic.input", "key-1", "value-1")).get();
		System.out.printf("test sent: %s%n", record);
		// receive message
		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", kafka.getEmbeddedKafka().getBrokersAsString());
		consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
		consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
		consumerProperties.put("group.id", getClass().getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
		consumer.subscribe(Collections.singleton("topic.output"));
		ConsumerRecords<String, String> records = null;
		while (records == null || records.isEmpty()) {
			records = consumer.poll(Duration.ofSeconds(1));
		}
		System.out.printf("test received: %s%n", records.iterator().next());
		// cleanup producer, consumer
		producer.close();
		consumer.close();
		// cancel job
		JobID jobId = flink.getMiniCluster().listJobs().get().iterator().next().getJobId();
		flink.getMiniCluster().cancelJob(jobId).get();
	}
}
