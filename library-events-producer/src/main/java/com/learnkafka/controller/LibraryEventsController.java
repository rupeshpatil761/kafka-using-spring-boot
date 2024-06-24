package com.learnkafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;

@RestController
public class LibraryEventsController {
	
	//private static final Logger logger = Logger.getLogger(LibraryEventsController.class.getName());
    private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent event){

		// invoke kafka producer
		logger.info("Library Event : {} ", event);
		
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
	}
	
}
