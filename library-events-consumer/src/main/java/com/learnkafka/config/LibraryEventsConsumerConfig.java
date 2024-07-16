package com.learnkafka.config;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka // not needed in latest version of kafka
public class LibraryEventsConsumerConfig {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(3);
		// factory.getContainerProperties().setAckMode(AckMode.MANUAL);
		// factory.setCommonErrorHandler(simpleErrorHandler());
		factory.setCommonErrorHandler(errorHandlerWithRetryListener());
		return factory;
	}

	// Default back off is 9 retries with no delay
	private DefaultErrorHandler simpleErrorHandler() {
		var fixedBackOff = new FixedBackOff(1000L, 2);
		return new DefaultErrorHandler(fixedBackOff);
	}

	private ExponentialBackOffWithMaxRetries exponentialBackOff() {
		ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1_000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2000L);
		return expBackOff;
	}

	private DefaultErrorHandler exponentialErrorHandler() {

		return new DefaultErrorHandler(exponentialBackOff());
	}

	private DefaultErrorHandler errorHandlerWithExceptionsToIgnoreOrRetry() {
		var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
		var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);
		var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2));

		// *** Either use addRetryableExceptions OR addNotRetryableExceptions at a time
		exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);
		// exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		return errorHandler;
	}

	// Error Handler with Exponential BackOff and RetryListener
	// Retry Listener (to monitor each Retry attempt) is not advisable to use in PROD env
	private DefaultErrorHandler errorHandlerWithRetryListener() {
		var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2));
		
		// RetryListener is Functional interface so passing it as lambda.
		errorHandler.setRetryListeners(((recod, ex, deliveryAttempt) -> {
			log.info("Failed record in retry listener , Exception : {} , deliveryAttempt : {} ", ex.getMessage(),
					deliveryAttempt);
		}));
		
		return errorHandler;
	}
}
