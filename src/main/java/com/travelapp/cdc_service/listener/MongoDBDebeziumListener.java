package com.travelapp.cdc_service.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelapp.cdc_service.dto.RoomPriceDto;
import com.travelapp.cdc_service.dto.StayRoomPriceUpdateDto;
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
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Component
public class MongoDBDebeziumListener {

    private final Executor executor;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(MongoDBDebeziumListener.class);

    @Autowired
    private PublisherService publisherService;

    public MongoDBDebeziumListener(Configuration mongoDBConnector) {
        this.executor = Executors.newSingleThreadExecutor();
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
        SourceRecord record = event.record();
        Struct value = (Struct) record.value();

        if (value == null) {
            logger.warn("Skipping empty change event: {}", record);
            return;
        }

        String op = value.getString(Constants.OPERATION);
        Struct source = value.getStruct(Constants.SOURCE);
        String collection = (source != null) ? source.getString(Constants.COLLECTION) : null;

        if (collection == null) {
            logger.debug("Skipping event with no collection: {}", record);
            return;
        }

        Object afterRaw = value.get("after");
        switch (collection.toUpperCase()) {
            case Constants.STAY_DETAIL: {
                if (Constants.CREATE_OP.equals(op)) {
                    handleCreateEvent(collection, afterRaw);
                } else if (Constants.UPDATE_OP.equals(op)) {
                    handleUpdateEvent(afterRaw, value);
                } else {
                    logger.debug("Ignoring unsupported operation: {}", op);
                }
            }
            break;
            default:
                logger.error("No matching data found");
        }
    }

    private void handleCreateEvent(String collection, Object afterRaw) {
        if (afterRaw instanceof String data) {
            logger.info("Insert event captured for collection {}: {}", collection, data);
            publisherService.publishStayDetailInfo(data, Constants.CREATE_OP);
        }
    }

    private void handleUpdateEvent(Object afterRaw, Struct value) {
        if (afterRaw == null) {
            logger.warn("Update event missing 'after' field, skipping");
            return;
        }

        Struct updateDescription = value.getStruct("updateDescription");
        if (updateDescription == null) {
            logger.warn("Update event missing 'updateDescription', skipping");
            return;
        }

        Map<String, Object> afterObj = parseJson(afterRaw.toString());
        if (afterObj == null) return;

        String stayId = (String) afterObj.get("stayId");
        if (stayId == null) {
            logger.warn("Update event without stayId: {}", afterObj);
            return;
        }

        Map<String, Object> updatedFields = extractUpdatedFields(updateDescription);
        String stayStatusUpdate = (String) updatedFields.getOrDefault(Constants.STAY_STATUS_UPDATE, null);
        if (Objects.nonNull(stayStatusUpdate)) {
            if (afterRaw instanceof String data) {
                publisherService.publishStayDetailInfo(data, Constants.UPDATE_OP);
            }
        }
        Set<RoomPriceDto> updatedRoomPrices = extractUpdatedRoomPrices(afterObj, updatedFields);

        if (!updatedRoomPrices.isEmpty()) {
            logger.info("Publishing {} room price updates for stayId={}", updatedRoomPrices.size(), stayId);
            publishStayRoomsPriceUpdates(Long.valueOf(stayId), updatedRoomPrices);
        }
    }

    private Map<String, Object> parseJson(String json) {
        try {
            return mapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse JSON: {}", json, e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractUpdatedFields(Struct updateDescription) {
        Object rawUpdatedFields = updateDescription.get("updatedFields");
        if (rawUpdatedFields instanceof String json) return parseJson(json);
        if (rawUpdatedFields instanceof Map<?, ?> map) return (Map<String, Object>) map;
        return Map.of();
    }

    @SuppressWarnings("unchecked")
    private Set<RoomPriceDto> extractUpdatedRoomPrices(Map<String, Object> afterObj, Map<String, Object> updatedFields) {
        Set<RoomPriceDto> roomPrices = new HashSet<>();

        for (var entry : updatedFields.entrySet()) {
            String field = entry.getKey();   // e.g. "rooms.0.price"
            Object newValue = entry.getValue();

            if (!field.startsWith("rooms.") || !field.endsWith(".price")) continue;

            String[] parts = field.split("\\.");
            int roomIndex = Integer.parseInt(parts[1]);

            List<Map<String, Object>> rooms = (List<Map<String, Object>>) afterObj.get("rooms");
            if (rooms == null || roomIndex >= rooms.size()) continue;

            Map<String, Object> room = rooms.get(roomIndex);
            Object roomIdObj = room.get("roomId");

            if (roomIdObj != null && newValue instanceof Number price) {
                roomPrices.add(new RoomPriceDto(Integer.parseInt(roomIdObj.toString()), price.doubleValue()));
            }
        }
        return roomPrices;
    }

    private void publishStayRoomsPriceUpdates(Long stayId, Set<RoomPriceDto> updatedRoomPrices) {
        publisherService.publishStayRoomPriceUpdates(new StayRoomPriceUpdateDto(stayId, updatedRoomPrices));
    }

    @PreDestroy
    private void stop() throws IOException {
        if (this.debeziumEngine != null) {
            this.debeziumEngine.close();
        }
    }
}
