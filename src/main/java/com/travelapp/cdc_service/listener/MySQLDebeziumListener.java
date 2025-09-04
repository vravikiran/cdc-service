package com.travelapp.cdc_service.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelapp.cdc_service.producer.service.ProducerService;
import com.travelapp.cdc_service.util.Constants;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class MySQLDebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    @Autowired
    ProducerService producerService;
    ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public MySQLDebeziumListener(Configuration userConnector) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(userConnector.asProperties()).notifying(t -> {
                    try {
                        handleChangeEvent(t);
                    } catch (JsonProcessingException e) {
                        log.error("Exception occurred while processing the event");
                    }
                }).build();
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent)
            throws JsonProcessingException {
        var sourceRecord = sourceRecordRecordChangeEvent.record();
        log.info("SourceRecord from mysql db {}", sourceRecord);
        Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
        Map<String, Object> payload = structToMap(sourceRecordChangeValue);
        log.info("payload:: {}", payload);
        String op = (String) payload.get(Constants.MYSQL_OPERATION);
        log.info("Operation ::  {}", op);
        String tableName = (String) payload.get(Constants.TABLE);
        log.info("tableName:: {}", tableName);
        if (tableName != null && op != null) {
            payload.remove(Constants.TABLE);
            if (!Constants.READ_OP.equals(op)) {
                payload.remove(Constants.MYSQL_OPERATION);
                publishRecordChange(tableName, objectMapper.writeValueAsString(payload), op);
            }
        }
    }

    public Map<String, Object> structToMap(Struct struct) {
        if (struct == null)
            return null;
        Map<String, Object> map = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            Object value = struct.get(field);
            if (value instanceof Struct) {
                map.put(field.name(), structToMap((Struct) value));
            } else {
                map.put(field.name(), value);
            }
        }
        return map;
    }

    private void publishRecordChange(String tableName, String payload, String op) {
        log.info("payload :: {}", payload);
        producerService.publishDataChanges(tableName, payload, op);
    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy
    private void stop() throws IOException {
        if (Objects.nonNull(this.debeziumEngine)) {
            this.debeziumEngine.close();
        }
    }

}