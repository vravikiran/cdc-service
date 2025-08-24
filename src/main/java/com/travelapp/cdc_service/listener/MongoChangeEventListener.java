package com.travelapp.cdc_service.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelapp.cdc_service.producer.service.PublisherService;
import com.travelapp.cdc_service.util.Constants;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Component
public class MongoChangeEventListener {

    private final Executor executor;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    @Autowired
    public PublisherService publisherService;
    Logger logger = LoggerFactory.getLogger(MongoChangeEventListener.class);

    public MongoChangeEventListener(Configuration mongoDBConnector) {
        this.executor = Executors.newSingleThreadExecutor();

        // Create embedded Debezium engine for MongoDB connector
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(mongoDBConnector.asProperties())
                .notifying(this::handleChangeEvent)
                .build();
    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> event) {
        SourceRecord sourceRecord = event.record();
        logger.info("Source Record :: {}", sourceRecord);
        Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
        if (sourceRecordChangeValue != null) {
            String op = sourceRecordChangeValue.getString(Constants.OPERATION);
            Struct source = sourceRecordChangeValue.getStruct(Constants.SOURCE);
            String collection = source.getString(Constants.COLLECTION);
            Object afterObj = sourceRecordChangeValue.get(Constants.AFTER);
            if (afterObj instanceof String data) {
                publishMessage(collection, op, data);
            }
        }
    }

    private void publishMessage(String collection, String operation, String data) {
        ObjectMapper mapper = new ObjectMapper();
        switch (collection.toUpperCase()) {
            case Constants.STAY_DETAIL: {
                logger.info("Captured message :: {} ",data);
                publisherService.publishStayDetailUpdates(data, operation);
                break;
            }
            default:
                logger.error("No matching data found");
        }
    }

    @PreDestroy
    private void stop() throws IOException {
        if (this.debeziumEngine != null) {
            this.debeziumEngine.close();
        }
    }
}
