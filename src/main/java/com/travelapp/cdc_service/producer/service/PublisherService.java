package com.travelapp.cdc_service.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelapp.cdc_service.dto.StayRoomPriceUpdateDto;
import com.travelapp.cdc_service.util.Constants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class PublisherService {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    Logger logger = LoggerFactory.getLogger(PublisherService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    public void publishStayDetailInfo(String data, String op) {
        logger.info("Operation information :: {}", op);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(Constants.STAY_DETAIL_TOPIC, null, data);
        producerRecord.headers().add(new RecordHeader("op", op.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(producerRecord);
        logger.info("successfully published the message in stay detail topic :: {}", data);
    }

    public void publishStayRoomPriceUpdates(StayRoomPriceUpdateDto stayRoomPriceUpdateDto) {
        try {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(Constants.STAY_ROOM_PRICE_UPDATES_TOPIC, null, objectMapper.writeValueAsString(stayRoomPriceUpdateDto));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage());
        }
    }

}
