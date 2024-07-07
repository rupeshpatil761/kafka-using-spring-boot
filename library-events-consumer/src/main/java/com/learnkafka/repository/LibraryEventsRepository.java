package com.learnkafka.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.learnkafka.entity.LibraryEvent;

@Repository
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer>{

}
