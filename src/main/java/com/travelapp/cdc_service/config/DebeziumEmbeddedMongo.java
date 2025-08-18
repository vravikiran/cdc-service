package com.travelapp.cdc_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import io.debezium.config.Configuration;

@Component
public class DebeziumEmbeddedMongo {

    @Bean
    public Configuration mongoDBConnector() {
        return
                Configuration.create()
                        .with("name", "mongodb-connector")
                        .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                        .with("mongodb.connection.string", "mongodb connection string")
                        .with("mongodb.ssl.enabled", "true")
                        .with("snapshot.mode", "initial")
                        .with("database.include.list", "travelapp")
                        .with("collection.include.list", "travelapp.StayDetail")
                        .with("value.converter.schemas.enable", "true")  // important for transform
                        .with("topic.prefix", "mongo-cdc")
                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                        .with("offset.storage.file.filename", "/tmp/mongodb-offsets.dat")
                        .with("offset.flush.interval.ms", "60000")
                        .with("topic.creation.default.replication.factor", 1)
                        .with("topic.creation.default.partitions", 1)
                        .build();
    }

}
