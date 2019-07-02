package com.example.lab4;

import com.example.lab4.domain.TransactionEvent;
import com.example.lab4.infra.TransactionEventRichFunction;
import com.example.lab4.infra.TransactionEventSourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Lab4Job {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
				.addSource(new TransactionEventSourceFunction())
				.keyBy(TransactionEvent::getTransactionId)
				.map(new TransactionEventRichFunction())
				.print();

		env.execute("lab");
	}
}
