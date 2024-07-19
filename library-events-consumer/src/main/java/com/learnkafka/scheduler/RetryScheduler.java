package com.learnkafka.scheduler;

import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.entity.LibraryEventStatus;
import com.learnkafka.repository.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;


    @Scheduled(fixedRate = 15000 ) // every 15 seconds
    public void retryFailedRecords(){
       log.info("Retrying Failed Records Started! Date: {} ", new Date());
       failureRecordRepository.findAllByStatus(LibraryEventStatus.RETRY.name())
        .forEach(failureRecord -> {
            try {
                var consumerRecord = buildConsumerRecord(failureRecord);
                try {
                	// our code will keep throwing same exception 
                	// so to just, skip re-processing same failed record, putting try-catch here  
                	libraryEventsService.processLibraryEvent(consumerRecord);
                } catch (Exception e) {
                	// ignore
				}
                failureRecord.setStatus(LibraryEventStatus.SKIPED.name());
                failureRecordRepository.save(failureRecord);
            } catch (Exception e){
                log.error("Exception in retryFailedRecords : {} ", e.getMessage(), e);
            }

        });
       log.info("Retrying Failed Records Completed! Date: {} ", new Date());
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(), failureRecord.getOffset_value(), failureRecord.getKey_value(),
                failureRecord.getErrorRecord());
    }
}
