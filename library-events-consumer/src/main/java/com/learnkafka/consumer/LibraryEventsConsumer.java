package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsConsumer.class);
	
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
		logger.info("Consumer Record: {}",consumerRecord);
	}

}
