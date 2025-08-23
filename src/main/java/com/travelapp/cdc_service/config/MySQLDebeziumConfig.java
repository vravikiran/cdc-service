package com.travelapp.cdc_service.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
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

    @Bean
    public io.debezium.config.Configuration userConnector() throws IOException {
        return io.debezium.config.Configuration.create()
                .with("name", "mysql-connector")
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("database.hostname", dbHost)
                .with("database.port", dbPort)
                .with("database.user", userName)
                .with("database.password", password)
                .with("database.dbname", database)
                .with("database.allowPublicKeyRetrieval", "true")
                .with("database.server.id", "10181")
                .with("database.server.name", "customer-mysql-db-server")

                // === Store offsets in Kafka ===
                .with("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore")
                .with("bootstrap.servers", "localhost:9092")
                .with("offset.storage.topic", "travelapp_mysql_consumer_offsets")
                .with("offset.storage.partitions", "1")
                .with("offset.storage.replication.factor", "1")
                .with("offset.flush.interval.ms", "60000")

                // === Internal schema history (Debezium 2.x) ===
                .with("schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory")
                .with("schema.history.internal.kafka.bootstrap.servers", "localhost:9092")
                .with("schema.history.internal.kafka.topic", "travelapp_internal_schema_history")
                .with("database.include.list", database)
                .with("table.include.list",
                        "localapp_user_mgmt.travel_agent_profile,localapp_user_mgmt.user_profile,localapp_user_mgmt.role")
                .with("column.include.list",
                        "localapp_user_mgmt.user_profile.mobileno_hash,localapp_user_mgmt.user_profile.email_hash,"
                                + "localapp_user_mgmt.user_profile.isactive,localapp_user_mgmt.user_profile.role_id,"
                                + "localapp_user_mgmt.user_profile.created_date,localapp_user_mgmt.user_profile.updated_date,"
                                + "localapp_user_mgmt.travel_agent_profile.mobileno_hash,localapp_user_mgmt.travel_agent_profile.email_hash,"
                                + "localapp_user_mgmt.travel_agent_profile.isactive,localapp_user_mgmt.travel_agent_profile.role_id,"
                                + "localapp_user_mgmt.travel_agent_profile.created_date,localapp_user_mgmt.travel_agent_profile.updated_date")
                .with("snapshot.mode", "schema_only")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter.schemas.enable", "false")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("key.converter.schemas.enable", "false")
                .with("topic.prefix", "ps_")
                .with("transforms", "unwrap")
                .with("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                .with("transforms.unwrap.add.fields", "op,table")
                .with("time.precision.mode", "connect")
                .build();
    }
}
