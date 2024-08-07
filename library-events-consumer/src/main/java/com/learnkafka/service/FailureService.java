package com.learnkafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.repository.FailureRecordRepository;

@Service
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus){
        var failureRecord = new FailureRecord(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                recordStatus);

        failureRecordRepository.save(failureRecord);

    }
}
