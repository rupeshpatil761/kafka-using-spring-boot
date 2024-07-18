package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.learnkafka.service.LibraryEventsServiceImpl;

@Component
//dont forgot to comment other consumer
public class LibraryEventsConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsConsumer.class);
	
	@Autowired
	private LibraryEventsServiceImpl libraryEventsService;
	
	// Spring boot auto configuration helps here to configure consumer
	// @@KafkaListener internally uses KafkaListenerContainerFactory & ConsumerFactory beans and they are responsible 
	// for reading the message from consumer
	@KafkaListener(topics = {"library-events"}, groupId = "${spring.kafka.consumer.group-id}")
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws Exception {
		logger.info("Consumer Record : {}",consumerRecord);
		libraryEventsService.processLibraryEvent(consumerRecord);
	}

}
