package com.learnkafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;

import jakarta.validation.Valid;

@RestController
public class LibraryEventsController {

	private final LibraryEventsProducer libraryEventsProducer;

	// constructor injection
	public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
		this.libraryEventsProducer = libraryEventsProducer;
	}

	// private static final Logger logger =
	// Logger.getLogger(LibraryEventsController.class.getName());
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<Object> postLibraryEvent(@RequestBody @Valid LibraryEvent event) {
		logger.info("Library Event : {} ", event);
		try {
			// invoke kafka producer
			// libraryEventsProducer.sendLibraryEvent(event);
			// libraryEventsProducer.sendLibraryEvent_approach2(event);
			libraryEventsProducer.sendLibraryEvent_approach3(event);

			// this statement will get printed even before sending event - when application
			// started and tries to send first event. i.e. asynchronous invocation of send
			// method
			logger.info("After sending library event");

			return ResponseEntity.status(HttpStatus.CREATED).body(event);
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	@PutMapping("/v1/libraryevent")
	public ResponseEntity<Object> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
		logger.info("PUT Request | Library Event : {} ", libraryEvent);
		try {
			ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
			if(BAD_REQUEST!=null) return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(BAD_REQUEST.getBody());
			
			libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);
			
			return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}
	
	private ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
		if (libraryEvent.libraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("please provide the libraryEventId");
		}
		if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
		}
		return null;
	}
}
