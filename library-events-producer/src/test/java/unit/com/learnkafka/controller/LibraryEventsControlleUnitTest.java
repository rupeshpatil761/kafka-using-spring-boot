package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventsProducer;
import com.learnkafka.util.TestUtil;

// Test Slice Concept -- Here we are slicing part of the application context i.e. Web Layer
// In case of Integration testing we have used whole Spring Boot Application context
// @WebMvcTest internally does @AutoConfigureWebMvc & @AutoConfigureMockMvc
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControlleUnitTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@MockBean
	private LibraryEventsProducer libraryEventsProducer;

	@Test
	void testPostLibraryEvent() throws Exception {
		// given
		var inputJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

		when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

		// when
		mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent").content(inputJson)
				.contentType(MediaType.APPLICATION_JSON)).andExpect(MockMvcResultMatchers.status().isCreated());

		// then
	}

	@Test
	void testPostLibraryEvent_InvalidInputs() throws Exception {
		// given
		var inputJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

		when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);
		
		var errorMessage = "book.bookId - must not be null,book.bookName - must not be blank";

		// when
		mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent").content(inputJson)
				.contentType(MediaType.APPLICATION_JSON))
		.andExpect(MockMvcResultMatchers.status().is4xxClientError())
		.andExpect(MockMvcResultMatchers.content().string(errorMessage));

		// then
	}
	
	@Test
	void testPutLibraryEvent() throws Exception {
		// given
		var inputJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdate());
		
		when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform((MockMvcRequestBuilders.put("/v1/libraryevent")).content(inputJson)
				.contentType(MediaType.APPLICATION_JSON))
				.andExpect(MockMvcResultMatchers.status().isOk());
		
		//then
	}
	
	@Test
	void updateLibraryEvent_withNullLibraryEventId() throws Exception {
		// given
		var inputJson = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdateWithNullLibraryEventId());
		when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform((MockMvcRequestBuilders.put("/v1/libraryevent")).content(inputJson)
				.contentType(MediaType.APPLICATION_JSON))
				.andExpect(MockMvcResultMatchers.status().is4xxClientError())
				.andExpect(MockMvcResultMatchers.content().string("please provide the libraryEventId"));
		// then
	}
	
	@Test
	void updateLibraryEvent_withNullInvalidEventType() throws Exception {
		// given
		var inputJson = objectMapper.writeValueAsString(TestUtil.newLibraryEventRecordWithLibraryEventId());
		when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);
		
		// when
		mockMvc.perform((MockMvcRequestBuilders.put("/v1/libraryevent")).content(inputJson)
				.contentType(MediaType.APPLICATION_JSON))
				.andExpect(MockMvcResultMatchers.status().is4xxClientError())
				.andExpect(MockMvcResultMatchers.content().string("Only UPDATE event type is supported"));

		// then
	}
}
