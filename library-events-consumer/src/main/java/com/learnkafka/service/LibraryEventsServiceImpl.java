package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventsRepository;

@Service
public class LibraryEventsServiceImpl implements LibraryEventsService {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsServiceImpl.class);
	
	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	private LibraryEventsRepository libraryEventsRepository;
	
	@Override
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws Exception {
		String eventType = "";
		try {
			LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
			eventType = libraryEvent.getLibraryEventType().name();
			log.info("LibraryEventsServiceImpl | libraryEvent : {}", libraryEvent);
			
			// This condition added to replicate the addRetryableExceptions scenario
			if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==999) {
				throw new RecoverableDataAccessException("Temporary network error");
			}
			
			switch (libraryEvent.getLibraryEventType()) {
			case NEW:
				save(libraryEvent);
				break;
			case UPDATE:
				validate(libraryEvent);
				save(libraryEvent);
				break;
			default:
				log.error("Invalid Library Event Type");
				break;
			}
		} catch (Exception e) {
			log.error("Failed to perform operation i.e. {} | Error msg: {}", eventType, e.getMessage());
			throw e;
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		if(libraryEvent.getLibraryEventId()==null) {
			throw new IllegalArgumentException("Library Event id is missing");
		}
		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
		if(!libraryEventOptional.isPresent()) {
			throw new IllegalArgumentException("Library event not found with given id: "+libraryEvent.getLibraryEventId());
		}
		log.info("Validation is successful for the library event : {}", libraryEventOptional.get());
	}
	
	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("Successfully persist the Library Event: {}", libraryEvent);
	}
}
