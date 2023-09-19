package com.opensource.flinkcdcservice.common.commons;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.TimeZone;

public interface ParamConstants {
    ConfigOption<Integer> PARALLELISM = ConfigOptions.key("thread")
            .intType()
            .defaultValue(1)
            .withDescription("execution thread num");

    ConfigOption<String> JOB_CHECKPOINT_MODE = ConfigOptions.key("checkpoint_mode")
            .stringType()
            .defaultValue("EXACTLY_ONCE")
            .withDescription("job checkpoint mode");

    ConfigOption<String> SRC_HOST = ConfigOptions.key("src_host")
            .stringType()
            .defaultValue("127.0.0.1")
            .withDescription("source db host");
    ConfigOption<Integer> SRC_PORT = ConfigOptions.key("source_port")
            .intType()
            .defaultValue(3306)
            .withDescription("source db port");
    ConfigOption<String> SRC_USER = ConfigOptions.key("src_user")
            .stringType()
            .defaultValue("root")
            .withDescription("source db user");
    ConfigOption<String> SRC_PWD = ConfigOptions.key("src_pwd")
            .stringType()
            .defaultValue("password")
            .withDescription("source db password");
    ConfigOption<String> SRC_DB = ConfigOptions.key("src_dbs")
            .stringType()
            .defaultValue("src_dbs")
            .withDescription("source tables, format is #{db}.#{table}, support regex");
    ConfigOption<String> SRC_TABLES = ConfigOptions.key("src_tables")
            .stringType()
            .defaultValue("src_tables")
            .withDescription("source db, format is #{db}, support regex");

    ConfigOption<Integer> FETCH_SIZE = ConfigOptions.key("fetch_size")
            .intType()
            .defaultValue(512)
            .withDescription("source db data fetch size");
    ConfigOption<Integer> CHUNK_SIZE = ConfigOptions.key("chunk_size")
            .intType()
            .defaultValue(1024)
            .withDescription("source db data chunk size");
    ConfigOption<String> TIME_ZONE = ConfigOptions.key("time_zone")
            .stringType()
            .defaultValue(TimeZone.getDefault().getID())
            .withDescription("source db time zone");

    // MySQL params
    ConfigOption<String> SRC_GTID = ConfigOptions.key("gtid")
            .stringType()
            .defaultValue(null)
            .withDescription("source db gtid set");

    ConfigOption<Boolean> FULL_DATA = ConfigOptions.key("full_transfer")
            .booleanType()
            .defaultValue(true)
            .withDescription("source do full transfer ");

    ConfigOption<String> SINK_HOST = ConfigOptions.key("sink_host")
            .stringType()
            .defaultValue("127.0.0.1")
            .withDescription("sink db host");
    ConfigOption<String> SINK_PORT = ConfigOptions.key("sink_port")
            .stringType()
            .defaultValue("3306")
            .withDescription("sink db port");
    ConfigOption<String> SINK_TOPIC = ConfigOptions.key("sink_topic")
            .stringType()
            .defaultValue(null)
            .withDescription("sink topic");
    ConfigOption<String> SINK_USER = ConfigOptions.key("sink_user")
            .stringType()
            .defaultValue("root")
            .withDescription("sink db user");
    ConfigOption<String> SINK_PWD = ConfigOptions.key("sink_pwd")
            .stringType()
            .defaultValue("password")
            .withDescription("sink db password");


    ConfigOption<String> SINK_KEY = ConfigOptions.key("sink_key")
            .stringType()
            .defaultValue("id")
            .withDescription("sink record key");
}
