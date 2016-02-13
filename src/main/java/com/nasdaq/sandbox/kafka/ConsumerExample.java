package com.nasdaq.sandbox.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConsumerExample {
	public static void main(String[] args) {
		Map<String, Object> props = new HashMap<String, Object>();
		// bootstrap.servers is required
		// key.serializer is required
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");

		Producer<String, Object> producer = new KafkaProducer<String, Object>(
				props);
		int i = 0;
	    
	     while(i<10) {
			producer.send(new ProducerRecord<String, Object>("MY-TOPIC",
					"value-" + i));
	      i++;
	     } 
	    
	    producer.close();
	}
}
