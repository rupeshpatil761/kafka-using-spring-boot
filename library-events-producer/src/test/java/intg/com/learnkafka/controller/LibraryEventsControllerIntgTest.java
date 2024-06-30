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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerIntgTest {
	
	@Autowired
	private TestRestTemplate testRestTemplate;
	
	// ## Use Embedded Kafka Producer  -- DONE
	// 1. configure Embedded kafka using @EmbeddedKafka annotation - so that Junit can create topic in embedded kafka
	// 2. override kafka bootstrap servers and Admin bootstrap brokers with spring.embedded.kafka.brokers using  @TestPropertySource annotation
	

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
	
	// Verification: Check bootstrap.servers in log i.e. [127.0.0.1:55649]


}
