package com.nasdaq.sandbox.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Writes messages to a topic.
 * 
 * @author grudkowm
 *
 */
public class ProducerExample {
	Producer<String, Object> producer = null;

	public static void main(String[] args) {

		ProducerExample example = new ProducerExample();
		example.send(100);

	}

	private void send(int n) {

		// Configure the producer
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");

		producer = new KafkaProducer<String, Object>(
				props);

		int i = 0;

		ProducerRecord<String, Object> record = null;
		while (i < 100) {

			// create a log record, specifying key
			// records having the same key are guaranteed
			// to be written to the same partition
			record = new ProducerRecord<String, Object>("mike", "5",
					("value" + i).getBytes());

			try {
				producer.send(record).get();
			} catch (Exception e) {
				System.out.println(e);
				// NOOP
			}
			i++;
		}

		producer.close();

	}
}
