package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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

	/**
	 * Approach 1  -- sync + async
	 * @param event
	 * @return
	 * @throws JsonProcessingException
	 * 
	 * 1. Blocking call - Very first send method gets the metadata of kafka cluster
	   (only one time after application boots up) -- sync call
	   2: Send message happens - Returns a CompletableFuture (async call)
	 */
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent event)
			throws JsonProcessingException {
		var key = event.libraryEventId();
		var value = objectMapper.writeValueAsString(event);

		var completableFuture = kafkaTemplate.send(topic, key, value);
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handlerFailure(key, value, throwable);
			} else {
				handleSuccess(key, value, sendResult);
			}
		});
	}

	// this will get printed based on acks value in cofig -- default is -1 or ALL
	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		logger.info("Message sent successfully for the key : {} and the value : {} , partition is {} ", key, value,
				sendResult.getRecordMetadata().partition());
	}

	private void handlerFailure(Integer key, String value, Throwable throwable) {
		logger.error("Error sending the message and exception is {}", throwable.getMessage(), throwable);
	}

	// Approach 2 -- sync
	// Pure synchronous approach - get() is a synchronous call
	public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws Exception {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);
		// OR simply call .get();
		handleSuccess(key, value, sendResult);
		return sendResult;

	}

	// Approach 3 - Using ProducerRecord with headers
	// similar behavior as approach 1 -- sync + async
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent event)
			throws Exception {
		
		var key = event.libraryEventId();
		var value = objectMapper.writeValueAsString(event);
		
		var producerRecord = buildProducerRecord(key, value);
		
		var completableFuture = kafkaTemplate.send(producerRecord);
		return completableFuture.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handlerFailure(key, value, throwable);
			} else {
				handleSuccess(key, value, sendResult);
			}
		});

	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		List<Header> recordsHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topic, null, key, value, recordsHeaders);
	}
}
