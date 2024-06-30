package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventsControllerIntgTest {
	
	@Autowired
	private TestRestTemplate testRestTemplate;

	@Test
	void testPostLibraryEvent() {
		// given 
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
		var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);
		
		// when
		var responseEntity = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);
		
		// then
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
	}

}
