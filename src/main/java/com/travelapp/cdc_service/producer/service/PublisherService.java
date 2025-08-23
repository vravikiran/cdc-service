package com.travelapp.cdc_service.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelapp.cdc_service.dto.StayDetailDto;
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
    ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Logger logger = LoggerFactory.getLogger(PublisherService.class);

    public void publishRequestUpdates(StayDetailDto stayDetailDto, String op) {
        logger.info("Operation information :: {}", op);
        try {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(Constants.STAY_DETAIL_TOPIC, null, objectMapper.writeValueAsString(stayDetailDto));
            producerRecord.headers().add(new RecordHeader("op", op.getBytes(StandardCharsets.UTF_8)));
            kafkaTemplate.send(producerRecord);
            logger.info("successfully published the message in stay detail topic :: {}", stayDetailDto.toString());
        } catch (JsonProcessingException e) {
            logger.error("Exception occurred while processing Json message {}", e.getMessage());
        }
    }
}
