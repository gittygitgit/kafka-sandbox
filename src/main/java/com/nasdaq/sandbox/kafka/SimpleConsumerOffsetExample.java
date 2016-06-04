package com.nasdaq.sandbox.kafka;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;

import com.nasdaq.sandbox.kafka.util.KafkaUtils;


/**
 * See https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+
 * Example.
 * 
 * Passes in a list of brokers. Just need to pass in a starting list. Any active
 * broker will know who the leader is for a given topic.
 * 
 * @author grudkowm
 *
 */
public class SimpleConsumerOffsetExample {

	private SimpleConsumer consumer = null;


	public SimpleConsumerOffsetExample() {
		super();

		PartitionMetadata returnedMetadata = null;
		try {
			// Create a simple consumer
			consumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024,
					"LeaderLookup");
			List<String> topics = new ArrayList<String>();
			topics.add("mike"); 

			List<String> brokerList = new ArrayList<String>();
			brokerList.add("localhost");
			brokerList.add("workmac");

			PartitionMetadata metadata = KafkaUtils.findLeader(brokerList,
					9092, "mike", 1);
			
			System.out.println(metadata.leader());
			
		} catch (Exception e) {
			System.out.println("Error");
		}
	}

	public static void main(String[] args) {

		SimpleConsumerOffsetExample example = new SimpleConsumerOffsetExample();

		example.getMetadata();
//		consumer.
//		Map<String, Object> props = new HashMap<String, Object>();
//		// bootstrap.servers is required
//		// key.serializer is required
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//				"org.apache.kafka.common.serialization.StringSerializer");
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//				"org.apache.kafka.common.serialization.StringSerializer");
//		props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
//
//		Producer<String, Object> producer = new KafkaProducer<String, Object>(
//				props);
//		int i = 0;
//	    
//	     while(i<10) {
//			producer.send(new ProducerRecord<String, Object>("MY-TOPIC",
//					"value-" + i));
//	      i++;
//	     } 
//	    
//	    producer.close();
	}

	private void getMetadata() {
	}
}
