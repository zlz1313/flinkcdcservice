package com.opensource.flinkcdcservice.common;

import com.opensource.flinkcdcservice.common.commons.ParamConstants;
import com.opensource.flinkcdcservice.common.utils.ParameterWrapper;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public final class MySQLSourceWrapper<T> {

    private MySqlSourceBuilder<T> builder;
    private ParameterWrapper parameter;
    public MySQLSourceWrapper(ParameterWrapper parameter) {
        this.builder = new Builder<T>(parameter).build();
        this.parameter = parameter;
        initJdbcProp();
        buildStartOptions();
        setPerformanceParam();
    }

    public String getSourceName() {
        return "MySQL Source";
    }

    public String getJobName() {
        return parameter.getString(ParamConstants.SRC_HOST);
    }

    static class Builder<T> {
        private final ParameterWrapper parameter;
        Builder(ParameterWrapper parameter) {
            this.parameter = parameter;
        }

        MySqlSourceBuilder<T> build() {
            MySqlSourceBuilder<T> mySqlSourceBuilder = MySqlSource.<T>builder()
                    .hostname(parameter.getString(ParamConstants.SRC_HOST))
                    .port(parameter.getInteger(ParamConstants.SRC_PORT))
                    .username(parameter.getString(ParamConstants.SRC_USER))
                    .password(parameter.getString(ParamConstants.SRC_PWD))
                    .databaseList(parameter.getString(ParamConstants.SRC_DB))
                    .tableList(parameter.getString(ParamConstants.SRC_TABLES))
                    .serverTimeZone(parameter.getString(ParamConstants.TIME_ZONE));
//            mySqlSourceBuilder.scanNewlyAddedTableEnabled(true);
            mySqlSourceBuilder.includeSchemaChanges(true);
            return mySqlSourceBuilder;
        }
    }

    private void initJdbcProp() {
        Properties properties = new Properties();
        // 解决MySQL 5.6低版本连接异常问题
        String sslMode = parameter.get("db.ssl.mode", "disabled");
        MySqlConnectorConfig.SecureConnectionMode secureConnectionMode =
                MySqlConnectorConfig.SecureConnectionMode.parse(sslMode);
        properties.put("useSSL", secureConnectionMode != MySqlConnectorConfig.SecureConnectionMode.DISABLED);
        this.builder.jdbcProperties(properties);
    }

    private void buildStartOptions() {
        String gtidSet = parameter.getString(ParamConstants.SRC_GTID);
        if (StringUtils.isNotEmpty(gtidSet)) {
            this.builder.startupOptions(StartupOptions.specificOffset(gtidSet));
            return;
        }
        if (!parameter.getBool(ParamConstants.FULL_DATA)) {
            this.builder.startupOptions(StartupOptions.latest());
        }
    }

    /**
     * default mysql full qps is 2K with 512 fetchSize 1024 chunkSize
     */
    private void setPerformanceParam() {
        this.builder.fetchSize(parameter.getInteger(ParamConstants.FETCH_SIZE));
        this.builder.splitSize(parameter.getInteger(ParamConstants.CHUNK_SIZE));
    }

    public void setDeserializer(DebeziumDeserializationSchema<T> deserializer, Properties properties) {
        this.builder.deserializer(deserializer);
        if (properties != null) {
            this.builder.debeziumProperties(properties);
        }
    }

    public MySqlSource<T> build() {
        return builder.build();
    }
}
