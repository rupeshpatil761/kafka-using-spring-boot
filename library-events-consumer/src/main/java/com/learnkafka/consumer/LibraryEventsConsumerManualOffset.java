package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
// dont forgot to comment out LibraryEventsConsumer
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsConsumerManualOffset.class);

	@Override
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
		logger.info("Consumer Record: {} | {} ",consumerRecord, "Manually Acknowleding msg" );
		acknowledgment.acknowledge();
	}
}
