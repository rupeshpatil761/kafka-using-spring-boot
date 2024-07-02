package com.learnkafka.controller;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class LibraryEventsControllerAdvice {
	
	private static final Logger logger = LoggerFactory.getLogger(LibraryEventsControllerAdvice.class);
	
	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<?> handleException(MethodArgumentNotValidException ex) {
		
		var errorMessage = ex.getBindingResult().getFieldErrors().stream()
				.map(error -> error.getField() + " - "+ error.getDefaultMessage())
				.sorted()
				.collect(Collectors.joining(","));
		
		logger.info("errorMessage : {} ", errorMessage);
		
		return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
	}
}
