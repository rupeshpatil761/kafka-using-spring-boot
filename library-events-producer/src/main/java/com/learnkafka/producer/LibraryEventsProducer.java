package com.learnkafka.producer;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

@Component
public class LibraryEventsProducer {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsProducer.class);
	
	@Value("${spring.kafka.topic}")
	String topic;
	
	private final KafkaTemplate<Integer, String> kafkaTemplate;
	
	private final ObjectMapper objectMapper;

	public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		super();
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}
	
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent event) throws JsonProcessingException {
		var key = event.libraryEventId();
		var value = objectMapper.writeValueAsString(event);
		var completableFuture =  kafkaTemplate.send(topic, key, value);
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if(throwable!=null) {
				handlerFailure(key, value, throwable);
			} else {
				handleSuccess(key, value, sendResult);
			}
		});
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		logger.info("Message sent successfully for the key : {} and the value : {} , partition is {} ", 
				key, value, sendResult.getRecordMetadata().partition());	
	}

	private void handlerFailure(Integer key, String value, Throwable throwable) {
		logger.error("Error sending the message and exception is {}", throwable.getMessage(), throwable);
	}
}
