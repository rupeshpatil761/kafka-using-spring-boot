package com.learnkafka.config;

import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka // not needed in latest version of kafka
public class LibraryEventsConsumerConfig {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);
	
	@Autowired
    KafkaTemplate kafkaTemplate;
	
	@Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;


    /**
     * You can configure the DefaultErrorHandler and DefaultAfterRollbackProcessor with a record recoverer when the maximum number of failures is reached for a record. 
     * The framework provides the DeadLetterPublishingRecoverer, which publishes the failed message to another topic
     * The recoverer requires a KafkaTemplate<Object, Object>, which is used to send the record.
     * The record sent to the dead-letter topic is enhanced with the additional headers
     */
    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
                , (r, e) -> {
            log.error("======Inside publishingRecoverer : {} | cause: {} ", e.getMessage(), e.getCause(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });
        return recoverer;
    }

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(3);
		// factory.getContainerProperties().setAckMode(AckMode.MANUAL);
		factory.setCommonErrorHandler(errorHandlerWithPublishingRecoverer());
		return factory;
	}

	
	/**
	 * It's used to configure retry policies where each subsequent retry attempt happens after a progressively increasing delay
	 * For example, if the initial retry delay is 1 second, the subsequent retries might be delayed by 2 seconds, 4 seconds, and so on
	 * Max Interval: The maximum delay between retries to avoid excessively long delays. 
	 */
	private DefaultErrorHandler exponentialBackOffErrorHandler() {
		ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1_000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2000L);
		return new DefaultErrorHandler(expBackOff);
	}

	
	// Error Handler with Fixed BackOff and RetryListener
	// Retry Listener to monitor each Retry attempt is not advisable to use in PROD env
	// DO NOT TOUCH THIS -- Can be used for publishModifyLibraryEvent_Null_LibraryEventId test case
	private DefaultErrorHandler errorHandlerWithRetryListener() {
		var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2));
		// RetryListener is Functional interface so passing it as lambda.
		errorHandler.setRetryListeners(((recod, ex, deliveryAttempt) -> {
			log.info("DefaultErrorHandler | Failed record in retry listener , Exception : {} , deliveryAttempt : {} ", ex.getMessage(),
					deliveryAttempt);
		}));
		return errorHandler;
	}
	
	// Default back off is 9 retries with no delay
	// DO NOT TOUCH THIS -- Can be used for publishModifyLibraryEvent_Null_LibraryEventId test case
	private DefaultErrorHandler simpleErrorHandler() {
		var fixedBackOff = new FixedBackOff(1000L, 2);
		return new DefaultErrorHandler(fixedBackOff);
	}

	// Can be used for publishModifyLibraryEvent_999_LibraryEventId test case
	private DefaultErrorHandler errorHandlerWithExceptionsToIgnoreOrRetry() {
		var fixedBackOff = new FixedBackOff(1000L, 2);
		var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
		var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);
		
		var errorHandler = new DefaultErrorHandler(fixedBackOff);

		// *** Either use addRetryableExceptions OR addNotRetryableExceptions at atime
		exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions); //
		//exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		return errorHandler;
	}
	
	// Can be used for publishModifyLibraryEvent_999_LibraryEventId_deadletterTopic test case
	private DefaultErrorHandler errorHandlerWithPublishingRecoverer() {
		var fixedBackOff = new FixedBackOff(1000L, 2);
		var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
		var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);
		
		var errorHandler = new DefaultErrorHandler(publishingRecoverer(), fixedBackOff);

		// *** Either use addRetryableExceptions OR addNotRetryableExceptions at atime
		exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions); //
		//exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		return errorHandler;
	}

}
