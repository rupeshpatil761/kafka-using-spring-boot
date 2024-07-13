package com.learnkafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
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
        //factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.setCommonErrorHandler(customErrorHandler());
        return factory;
    }

	// Default back off is 9 retries with no delay 
	private DefaultErrorHandler customErrorHandler() {
		var fixedBackOff = new FixedBackOff(1000l, 2);
		var errorHandler = new DefaultErrorHandler(fixedBackOff);
		
		// ** Do not use this in prod env. THis is for just debugging purpose in dev env
		// RetryListener is Functional interface so passing it as lambda.
		errorHandler.setRetryListeners(((recod, ex, deliveryAttempt) -> {
			log.info("Failed record in retry listener , Exception : {} , deliveryAttempt : {} ",ex.getMessage(), deliveryAttempt);
		}));
		return errorHandler;
	}
	
}
