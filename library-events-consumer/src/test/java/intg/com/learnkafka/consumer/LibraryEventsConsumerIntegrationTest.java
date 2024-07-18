package com.learnkafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.repository.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"
        , "library-events.RETRY"
        , "library-events.DLT"
})
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "retryListener.startup=false"})
public class LibraryEventsConsumerIntegrationTest {
	
	@Value("${topics.retry}")
	private String retryTopic;

	@Value("${topics.dlt}")
	private String deadLetterTopic;
	
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	// This has hold of all the listener containers
	// In our case, LibraryEventsConsumer is our listener container
	private KafkaListenerEndpointRegistry endpointRegistry;
	
	@SpyBean
	private LibraryEventsConsumer libraryEventsConsumerSpy;
	
	@SpyBean
	private LibraryEventsService libraryEventsServiceSpy;
	
	@Autowired
	private LibraryEventsRepository libraryEventsRepository;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	private Consumer<Integer, String> consumer;
	
	@BeforeEach
	void setUp() {
		// setup to read when all consumers are up and ready to consume and wait until all partitions are assigned to it
		/*
		 * for(MessageListenerContainer messageListenerContainer :
		 * endpointRegistry.getListenerContainers()) {
		 * ContainerTestUtils.waitForAssignment(messageListenerContainer,
		 * embeddedKafkaBroker.getPartitionsPerTopic()); }
		 */
		
		// As we are disabling LibraryEventsRetryConsumer as part of this test case, 
		// we have to wait for assignment of LibraryEventsConsumer only now
		// Otherwise, we will get error saying: Expected 2 but got 0 partitions for LibraryEventsRetryConsumer listener container
		var container = endpointRegistry.getListenerContainers().stream().filter(
				listenerContainer -> Objects.equals(listenerContainer.getGroupId(), "library-events-listener-group"))
				.collect(Collectors.toList()).get(0);
		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
	}
	
	@AfterEach
	void tearDown() {
		// As H2 database is shared database we might run into data issues
		// so delete the data after each test case
		libraryEventsRepository.deleteAll();
	}
	
	@Test
	@Disabled
	// flow :: sendDefault() --> kafka topic --> consumer --> service
	void publishNewLibraryEvent() throws Exception {
		// given
		String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(json).get();
		
		// when
		// As we know, consumer runs as a separate thread from application thread.
		// So after sendDefault call it might take a while for consumer to read the record
		// CountDownLatch helps to block current execution thread and its handy when we are writing async test cases
		CountDownLatch latch = new CountDownLatch(1);
		// as soon as count goes down it will release the current thread.
		latch.await(3, TimeUnit.SECONDS);
		
		// then
		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventsList = (List<LibraryEvent>) libraryEventsRepository.findAll();
		assert libraryEventsList.size() == 1;
		libraryEventsList.forEach(event -> {
			assert event.getLibraryEventId() != null;
			assertEquals(456, event.getBook().getBookId());
		});
		
	}

	
	@Test
	@Disabled
	// flow :: sendDefault() --> kafka topic --> consumer --> service
	void publishUpdateLibraryEvent() throws Exception {
		//given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        
        //publish the update LibraryEvent
        Book updatedBook = Book.builder().
                bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();
        
        //when
       CountDownLatch latch = new CountDownLatch(1);
       latch.await(3, TimeUnit.SECONDS);

       //then
       verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
       verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
       
       LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
       assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
	}
	
	@Test
	@Disabled
	// Use errorHandlerWithRetryListener or simple error handler
    void publishModifyLibraryEvent_Null_LibraryEventId() throws Exception {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // times 10 because default error handling will try for 10 times
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
	}
	
	// Test case to replicate the addRetryableExceptions scenario
	// Use errorHandlerWithExceptionsToIgnoreOrRetry handler
	@Test
	@Disabled
    void publishModifyLibraryEvent_999_LibraryEventId() throws Exception {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
    }
	
	@Test
    void publishModifyLibraryEvent_999_LibraryEventId_deadletterTopic() throws Exception {
        //given
        Integer libraryEventId = 999;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Without LibraryEventsRetryListener Consumer -- make sure you disable this consumer while using times(3)
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        System.out.println("consumer Record in deadletter topic : " + consumerRecord.value() +" | retryTopic: "+retryTopic);

        assertEquals(json, consumerRecord.value());
        
        consumerRecord.headers()
        .forEach(header -> {
            System.out.println("Header Key : " + header.key() + ", Header Value : " + new String(header.value()));
        });
    }
}
