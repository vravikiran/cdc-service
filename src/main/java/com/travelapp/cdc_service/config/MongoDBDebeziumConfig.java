package com.travelapp.cdc_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import io.debezium.config.Configuration;

@Component
public class MongoDBDebeziumConfig {

    @Bean
    public Configuration mongoDBConnector() {
        return
                Configuration.create()
                        .with("name", "mongodb-connector")
                        .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                        .with("mongodb.connection.string", "mongodb connection url")   // your full URI
                        .with("mongodb.ssl.enabled", "true")
                        .with("snapshot.mode", "never")            // only schema snapshot, no initial docs
                        .with("database.include.list", "travelapp")
                        .with("collection.include.list", "travelapp.StayDetail")
                        .with("topic.prefix", "mongo-cdc")

                        // Offset storage in Kafka
                        .with("bootstrap.servers", "localhost:9092")
                        .with("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore")
                        .with("offset.storage.topic", "stay_service_offsets")
                        .with("offset.storage.partitions", "1")
                        .with("offset.storage.replication.factor", "1")
                        .with("offset.flush.interval.ms", "10000")

                        // Optional topic auto-creation defaults
                        .with("topic.creation.default.replication.factor", 1)
                        .with("topic.creation.default.partitions", 1)

                        .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .with("value.converter.schemas.enable", "true")
                        .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .with("key.converter.schemas.enable", "true")
                        .build();
    }

}
