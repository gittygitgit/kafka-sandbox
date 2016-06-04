package com.nasdaq.sandbox.kafka;

import java.util.ArrayList;
import java.util.List;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;


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
public class TopicMetadataExample {

	private SimpleConsumer consumer = null;


	public TopicMetadataExample() {
		super();

		PartitionMetadata returnedMetadata = null;
		try {
			// Create a simple consumer
			consumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024,
					"LeaderLookup");
			List<String> topics = new ArrayList<String>();
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			

			// Request information from kafka
			// No topic is targetted
			kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
			
			List<TopicMetadata> topicsMetadata = resp.topicsMetadata();
			for (TopicMetadata topicMetadata : topicsMetadata) {
				// there's a separate TopicMetadata for each topic present on the 
				// targetted broker.
				System.out.println(topicMetadata.topic());
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {

					// There's a separate PartitionMetadata for each partition in
					// the given topic
					Broker leader = partitionMetadata.leader();
					System.out.println(leader.connectionString());
					System.out.println(leader);
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
			System.out.println(e);
		}
	}

	public static void main(String[] args) {

		TopicMetadataExample example = new TopicMetadataExample();

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
