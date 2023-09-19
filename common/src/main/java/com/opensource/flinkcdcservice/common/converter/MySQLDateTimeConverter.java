package com.opensource.flinkcdcservice.common.converter;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class MySQLDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    @Override
    public void configure(Properties properties) {

    }

    @Override
    public void converterFor(RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String sqlType = relationalColumn.typeName().toUpperCase();
        if ("DATETIME".equals(sqlType)) {
            SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("com.darcyrech.debezium.special.datetime.string");
            converterRegistration.register(datetimeSchema, this::convertDateTime);
        }
    }

    private String convertDateTime(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof LocalDateTime) {
            return DATE_TIME_FORMAT.format((LocalDateTime)input);
        } else if (input instanceof Timestamp) {
            LocalDateTime time = ((Timestamp)input).toLocalDateTime();
            return DATE_TIME_FORMAT.format(time);
        }
        return String.valueOf(input);
    }
}
