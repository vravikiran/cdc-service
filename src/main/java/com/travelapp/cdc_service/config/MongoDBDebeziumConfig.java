package com.travelapp.cdc_service.config;


import com.travelapp.cdc_service.util.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import io.debezium.config.Configuration;

@Component
public class MongoDBDebeziumConfig {

    @Value("${mongodb.connection.url}")
    private String mongoDBConnStr;
    @Value("${mongodb.snapshot.mode}")
    private String snapshotMode;
    @Value("${mongodb.database.include.list}")
    private String databaseIncludeList;
    @Value("${mongodb.collection.include.list}")
    private String collectionIncludeList;
    @Value("${mongodb-connector.topic.prefix}")
    private String topicPrefix;
    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServer;
    @Value("${kafka.offset.storage}")
    private String kafkaOffsetStorage;
    @Value("${mongodb-connector.offset.storage.topic}")
    private String offsetStorageTopic;
    @Value("${mongodb-connector.offset.storage.partitions}")
    private int offsetStoragePartitions;
    @Value("${mongodb-connector.offset.storage.replication.factor}")
    private int offsetStorageRepFactor;
    @Value("${mongodb-connector.offset.flush.interval.ms}")
    private int offsetFlushInterval;
    @Value("${kafka.topic.creation.default.replication.factor}")
    private int defaultRepFactor;
    @Value("${kafka.topic.creation.default.partitions}")
    private int defaultNoOfPartitions;
    @Value("${kafka.value.converter}")
    private String valueConverter;
    @Value("${kafka.key.converter}")
    private String keyConverter;

    @Bean
    public Configuration mongoDBConnector() {
        return
                Configuration.create()
                        .with(Constants.CONNECTOR_NAME_PROP, Constants.MONGODB_CONNECTOR_NAME)
                        .with(Constants.CONNECTOR_CLASS_PROP, Constants.MONGODB_CONNECTOR_CLASS)
                        .with(Constants.MONGODB_CONNECTION_STR_PROP, mongoDBConnStr)   // your full URI
                        .with(Constants.MONGODB_CONNECTOR_SSL_PROP, Boolean.TRUE)
                        .with(Constants.SNAPSHOT_MODE_PROP, snapshotMode)            // only schema snapshot, no initial docs
                        .with(Constants.DB_INCLUDE_LIST_PROP, databaseIncludeList)
                        .with(Constants.COLLECTION_INCLUDE_LIST_PROP, collectionIncludeList)
                        .with(Constants.TOPIC_PREFIX_PROP, topicPrefix)

                        // Offset storage in Kafka
                        .with(Constants.BOOTSTRAP_SERVERS_PROP, kafkaBootstrapServer)
                        .with(Constants.OFFSET_STORAGE_PROP, kafkaOffsetStorage)
                        .with(Constants.OFFSET_STORAGE_TOPIC_PROP, offsetStorageTopic)
                        .with(Constants.OFFSET_STORAGE_PARTITIONS_PROP, offsetStoragePartitions)
                        .with(Constants.OFFSET_STORAGE_REP_FACTOR_PROP, offsetStorageRepFactor)
                        .with(Constants.OFFSET_FLUSH_INTERVAL_PROP, offsetFlushInterval)

                        // Optional topic auto-creation defaults
                        .with(Constants.TOPIC_CREATION_DEFAULT_REP_FACTOR_PROP, defaultRepFactor)
                        .with(Constants.TOPIC_CREATION_DEFAULT_PARTITIONS_PROP, defaultNoOfPartitions)

                        .with(Constants.VALUE_CONVERTER_PROP, valueConverter)
                        .with(Constants.VALUE_CONVERTER_SCHEMAS_ENABLE_PROP, Boolean.TRUE)
                        .with(Constants.KEY_CONVERTER_PROP, keyConverter)
                        .with(Constants.KEY_CONVERTER_SCHEMAS_ENABLE_PROP, Boolean.TRUE)
                        .build();
    }

}
