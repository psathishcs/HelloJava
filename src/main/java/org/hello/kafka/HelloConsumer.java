/**
 * 
 */
package org.hello.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author Sathish Kumar Parthasarathy
 *
 * Sep 12, 2016 8:36:45 PM
 *
 */
public class HelloConsumer {
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}
		String topicName = args[0].toString();
		Properties props = new Properties();

		// Assign the server detail 
		props.put("bootstrap.servers", "localhost:9092");
		
		props.put("group.id", "test");
		
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String ,String> consumer = new KafkaConsumer<String, String>(props);
		
		//Kafka Consumer subscribes list of topic here.
		consumer.subscribe(Arrays.asList(topicName));
		
	}
}
