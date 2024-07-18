package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.learnkafka.service.LibraryEventsServiceImpl;

@Component
public class LibraryEventsRetryConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsRetryConsumer.class);
	
	@Autowired
	private LibraryEventsServiceImpl libraryEventsService;
	
	@KafkaListener(topics = {"${topics.retry}"}, groupId = "retry-listener-group")
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws Exception {
		logger.info("Consumer Record in Retry Consumer : {}",consumerRecord);
		// With the below call,  retry consumer will going into loop 
		// because we are trying to process same message and libraryEventsService will keep throwing the exception
		// ** To verify that our retry consumer is working, we can simply check for the log in console.
		libraryEventsService.processLibraryEvent(consumerRecord);
	}
	
	/**
	 * Anytime we configure multiple consumers, the group id provided in application.yml is not going to work
	 * So, its always recommended to add a group id in @KafkaListener annotation
	 * By configuring seperate group id for each consumer we can easily re-consuming the messages from topic
	 */

}
