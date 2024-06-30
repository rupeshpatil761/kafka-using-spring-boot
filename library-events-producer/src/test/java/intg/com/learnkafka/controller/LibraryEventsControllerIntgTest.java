package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsControllerIntgTest {
	
	@Autowired
	private TestRestTemplate testRestTemplate;
	
	// ## Use Embedded Kafka Producer in test case -- DONE
	// 1. configure Embedded kafka using @EmbeddedKafka annotation - so that Junit can create topic in embedded kafka
	// 2. override kafka bootstrap servers and Admin bootstrap brokers with spring.embedded.kafka.brokers using  @TestPropertySource annotation
	

	// ## Use Embedded Kafka Consumer in test case
	// 1. Wire KafkaConsumer and EmbeddedKafkaBroker
	// 2. Consume the record from the EmbeddedKafkaBroker and then assert on it.
	
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	private Consumer<Integer, String> consumer;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	
	@BeforeEach
	void setUp() {
		// Configure/wired Kafka Consumer to read from Embedded Broker. 
		// 2nd argument is autoCommit=true
		var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
				.createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
	@AfterEach
	void tearDown() {
		consumer.close();
	}
	
	
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
		
		ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
		assert consumerRecords.count() == 1;
		
		consumerRecords.forEach(record -> {
			var actualRecord = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
			System.out.println("libraryEventActualRecord : "+actualRecord);
			assertEquals(actualRecord, TestUtil.libraryEventRecord());
		});
	}
	
	// Verification of embedded broker: Check bootstrap.servers in log i.e. [127.0.0.1:55649]

}
