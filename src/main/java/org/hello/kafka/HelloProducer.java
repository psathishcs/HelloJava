/**
 * 
 */
package org.hello.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Sathish Kumar Parthasarathy
 *
 * Sep 12, 2016 7:37:30 PM
 *
 */
public class HelloProducer {
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0){
			System.out.println("Enter topic name");
			return;
		}
		String topicName = args[0].toString();
		Properties props = new Properties();
		
		// Assign the server detail 
		props.put("bootstrap.servers", "localhost:9092");
		
		//Set acknowledgements for producer requests
		props.put("acks", "all");
		
		//IF the request fails, the producer can automatically retry
		props.put("retries", 0);
		
		//Specify buffer size in config
		props.put("batch.size", 16384);
		
		//Reduce the no of request less that 0
		props.put("linger.ms", 1);
		
		// The buffer memory controls the total amount of memory avaliables
		props.put("buffer.memory", 33554432);
		
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String,String> producer = new KafkaProducer<String, String>(props);
		
		for (int i = 0; i < 10; i++){
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i),Integer.toString(i)));
			System.out.println("Message Send successfully!");
			producer.close();
		}
		
	}
}
