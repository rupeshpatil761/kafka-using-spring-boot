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
	
	// Explanation for when() call in testPostLibraryEvent() test case
	
	/***
	 * The when() call in your unit test case is significant because it allows you
	 * to mock the behavior of the
	 * libraryEventsProducer.sendLibraryEvent_approach3() method. In this case,
	 * since the method is asynchronous and doesn't return anything of interest for
	 * the test, using thenReturn(null) is sufficient.
	 * 
	 * By mocking this method, you are isolating the behavior of the method being
	 * tested without actually invoking its real functionality, which helps in
	 * focusing on the specific unit being tested. While you mentioned that the test
	 * case runs without this line of code, it's important to include it to ensure
	 * that the behavior of the method being tested is controlled and predictable
	 * within the test environment.
	 * 
	 * This practice is common in unit testing to ensure that the test is reliable
	 * and independent of external factors.
	 * 
	 * Keep in mind that the purpose of unit testing is not only to check if the
	 * code runs but also to verify its behavior under different scenarios. If you
	 * have any more questions or need further clarification, feel free to ask.
	 */

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
