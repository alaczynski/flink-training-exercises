package com.example.sandbox;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaJob {

	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameters);
		env.setParallelism(1);
		env.disableOperatorChaining();

		// consumer
		DeserializationSchema<String> deserializationSchema = new SimpleStringSchema();
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameters.get("bootstrap.servers"));
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
				"topic.input",
				deserializationSchema,
				consumerProperties
		);
		consumer.setStartFromEarliest();

		// producer
		SerializationSchema<String> serializationSchema = new SimpleStringSchema();
		Properties producerProperties = new Properties();
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parameters.get("bootstrap.servers"));
		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
				"topic.output",
				serializationSchema,
				producerProperties
		);
		env
				.addSource(consumer)
				.map((value) -> {
					System.out.println("job: " + value);
					return value.toUpperCase();
				})
				.addSink(producer);
		env.execute();
	}
}
