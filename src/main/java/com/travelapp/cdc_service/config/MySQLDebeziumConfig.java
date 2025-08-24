package com.travelapp.cdc_service.config;

import com.travelapp.cdc_service.util.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class MySQLDebeziumConfig {
    @Value("${user.datasource.host}")
    private String dbHost;
    @Value("${user.datasource.port}")
    private String dbPort;
    @Value("${user.datasource.username}")
    private String userName;
    @Value("${user.datasource.password}")
    private String password;
    @Value("${user.datasource.database}")
    private String database;
    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServer;
    @Value("${mysql-connector-time.precision.mode}")
    private String timePrecisionMode;
    @Value("${mysql-connector.unwrap.add.fields}")
    private String unWrapFields;
    @Value("${mysql-connector.unwrap.type}")
    private String unwrapType;
    @Value("${mysql-connector.topic.prefix}")
    private String topicPrefix;
    @Value("${mysql-connector.transforms.prop}")
    private String transformsProp;
    @Value("${kafka.offset.storage}")
    private String kafkaOffsetStorage;
    @Value("${mysql-connector.offset.storage.topic}")
    private String offsetStorageTopic;
    @Value("${mysql-connector.offset.storage.partitions}")
    private int offsetStoragePartitions;
    @Value("${mysql-connector.offset.storage.replication.factor}")
    private int offsetStorageRepFactor;
    @Value("${mysql-connector.offset.flush.interval.ms}")
    private int offsetFlushInterval;
    @Value("${mysql-connector.schema.history.internal}")
    private String schemaHistoryInternal;
    @Value("${mysql-connector.schema.history.internal.kafka.topic}")
    private String schemaHistoryInternalTopic;
    @Value("${kafka.value.converter}")
    private String valueConverter;
    @Value("${kafka.key.converter}")
    private String keyConverter;
    @Value("${mysql-connector.db-server-id}")
    private String mysqlDBServerId;
    @Value("${mysql-connector.db-server-name}")
    private String mysqlDBServerName;
    @Value("${mysql-connector.table.include.list}")
    private String tablesList;
    @Value("${mysql-connector.table.col.include.list}")
    private String tableColsList;
    @Value("${mysql-connector.snapshot.mode}")
    private String mySqlSnapshotMode;

    @Bean
    public io.debezium.config.Configuration userConnector() throws IOException {
        return io.debezium.config.Configuration.create()
                .with(Constants.CONNECTOR_NAME_PROP, Constants.MYSQL_CONNECTOR_NAME)
                .with(Constants.CONNECTOR_CLASS_PROP, Constants.MYSQL_CONNECTOR_CLASS)
                .with(Constants.MYSQL_DB_HOST_NAME_PROP, dbHost)
                .with(Constants.MYSQL_DB_PORT_PROP, dbPort)
                .with(Constants.MYSQL_DB_USER_NAME_PROP, userName)
                .with(Constants.MYSQL_DB_PASSWORD_PROP, password)
                .with(Constants.MYSQL_DB_NAME_PROP, database)
                .with(Constants.MYSQL_DB_ALLOW_PUBLIC_KEY_RETRIEVAL_PROP, Boolean.TRUE)
                .with(Constants.MYSQL_DB_SERVER_ID_PROP, mysqlDBServerId)
                .with(Constants.MYSQL_DB_SERVER_NAME_PROP, mysqlDBServerName)

                // === Store offsets in Kafka ===
                .with(Constants.OFFSET_STORAGE_PROP, kafkaOffsetStorage)
                .with(Constants.BOOTSTRAP_SERVERS_PROP, kafkaBootstrapServer)
                .with(Constants.OFFSET_STORAGE_TOPIC_PROP, offsetStorageTopic)
                .with(Constants.OFFSET_STORAGE_PARTITIONS_PROP, offsetStoragePartitions)
                .with(Constants.OFFSET_STORAGE_REP_FACTOR_PROP, offsetStorageRepFactor)
                .with(Constants.OFFSET_FLUSH_INTERVAL_PROP, offsetFlushInterval)

                // === Internal schema history (Debezium 2.x) ===
                .with(Constants.SCHEMA_HISTORY_INTERNAL_PROP, schemaHistoryInternal)
                .with(Constants.SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVER_PROP, kafkaBootstrapServer)
                .with(Constants.SCHEMA_HISTORY_INTERNAL_KAFKA_TOPIC_PROP, schemaHistoryInternalTopic)
                .with(Constants.DB_INCLUDE_LIST_PROP, database)
                .with(Constants.TABLE_INCLUDE_LIST_PROP,
                        tablesList)
                .with(Constants.COL_INCLUDE_LIST_PROP, tableColsList)
                .with(Constants.SNAPSHOT_MODE_PROP, mySqlSnapshotMode)
                .with(Constants.VALUE_CONVERTER_PROP, valueConverter)
                .with(Constants.VALUE_CONVERTER_SCHEMAS_ENABLE_PROP, Boolean.TRUE)
                .with(Constants.KEY_CONVERTER_PROP, keyConverter)
                .with(Constants.KEY_CONVERTER_SCHEMAS_ENABLE_PROP, Boolean.TRUE)
                .with(Constants.TOPIC_PREFIX_PROP, topicPrefix)
                .with(Constants.TRANSFORMS_PROP, transformsProp)
                .with(Constants.TRANSFORMS_UNWRAP_TYPE_PROP, unwrapType)
                .with(Constants.TRANSFORMS_UNWRAP_ADD_FIELDS_PROP, unWrapFields)
                .with(Constants.TIME_PRECISION_MODE_PROP, timePrecisionMode)
                .build();
    }
}
