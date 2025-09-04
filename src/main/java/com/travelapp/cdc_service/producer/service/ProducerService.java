package com.travelapp.cdc_service.producer.service;

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
public class ProducerService {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    Logger logger = LoggerFactory.getLogger(ProducerService.class);

    public void publishDataChanges(String tableName, String payload, String op) {

        switch (tableName) {
            case Constants.USER_MGMT_USER_PROFILE_TABLE: {
                logger.info("publishing record received from table {} into topic {}", tableName, Constants.USER_MGMT_USER_PROFILE_TOPIC);
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(Constants.USER_MGMT_USER_PROFILE_TOPIC, null, payload);
                producerRecord.headers().add(new RecordHeader(Constants.OPERATION, op.getBytes(StandardCharsets.UTF_8)));
                kafkaTemplate.send(producerRecord);
                logger.info("Record published into topic {}", Constants.USER_MGMT_USER_PROFILE_TOPIC);
                break;
            }
            case Constants.TRAVEL_AGENT_PROFILE_TABLE: {
                logger.info("publishing record received from table {} into topic {}", tableName, Constants.TRAVEL_AGENT_PROFILE_TOPIC);
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(Constants.TRAVEL_AGENT_PROFILE_TOPIC, null, payload);
                producerRecord.headers().add(new RecordHeader(Constants.OPERATION, op.getBytes(StandardCharsets.UTF_8)));
                kafkaTemplate.send(producerRecord);
                logger.info("Record published into topic {}", Constants.TRAVEL_AGENT_PROFILE_TOPIC);
                break;
            }
            case Constants.ROLE_TABLE: {
                logger.info("publishing record received from table {} into topic {}", tableName, Constants.ROLE_TOPIC);
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(Constants.ROLE_TOPIC, null, payload);
                producerRecord.headers().add(new RecordHeader(Constants.OPERATION, op.getBytes(StandardCharsets.UTF_8)));
                kafkaTemplate.send(producerRecord);
                logger.info("Record published into topic {}", Constants.ROLE_TOPIC);
                break;
            }
            default: {
                logger.error("No table exists with given tableName :: {}", tableName);
            }
        }
    }
}
