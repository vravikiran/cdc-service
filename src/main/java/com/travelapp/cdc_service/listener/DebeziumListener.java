package com.travelapp.cdc_service.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class DebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    //ProducerService producerService;
    ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public DebeziumListener(Configuration userConnector) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(userConnector.asProperties()).notifying(t -> {
                    try {
                        handleChangeEvent(t);
                    } catch (JsonProcessingException e) {
                        // TODO Auto-generated catch block
                        log.error("Exception occurred while processing the event");
                    }
                }).build();
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent)
            throws JsonProcessingException {
        var sourceRecord = sourceRecordRecordChangeEvent.record();
        System.out.println("SourceRecord :: from mysql db " + sourceRecord);
        Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
        Map<String, Object> payload = structToMap(sourceRecordChangeValue);
        System.out.println("payload:: " + payload);
        String op = (String) payload.get(Constants.OPERATION);
        System.out.println(op);
        String tableName = (String) payload.get(Constants.MYSQL_OPERATION);
        System.out.println("tableName:: " + tableName);
        if (tableName != null && op != null) {
            payload.remove(Constants.TABLE);
            if (!Constants.READ_OP.equals(op)) {
                publishRecordChange(tableName, payload);
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

    private void publishRecordChange(String tableName, Map<String, Object> payload) {
        log.info("table name :: {}", tableName);
        log.info("payload :: {}", payload);
        //producerService.publishDataChanges(tableName, payload);
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