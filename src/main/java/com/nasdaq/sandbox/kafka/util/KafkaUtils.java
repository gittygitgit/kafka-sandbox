package com.nasdaq.sandbox.kafka.util;

import java.util.Collections;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaUtils {

	public static PartitionMetadata findLeader(List<String> replicas,
			int a_port, String a_topic, int a_partition) {
		return KafkaUtils.findLeader(null, replicas, a_port, a_topic,
				a_partition);
	}
	public static PartitionMetadata findLeader(List<String> a_seedBrokers,
			List<String> replicas,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;

		if (a_seedBrokers == null) {
			a_seedBrokers = replicas;
		}
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
						"leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			replicas.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicas.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
