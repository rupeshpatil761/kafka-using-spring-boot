package com.learnkafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public interface LibraryEventsService {

	void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord);
}
