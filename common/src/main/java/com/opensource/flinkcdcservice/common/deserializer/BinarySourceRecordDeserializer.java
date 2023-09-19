package com.opensource.flinkcdcservice.common.deserializer;

import com.opensource.flinkcdcservice.common.record.BinarySourceRecord;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class BinarySourceRecordDeserializer implements DebeziumDeserializationSchema<BinarySourceRecord> {

    private final boolean isCanal;

    public BinarySourceRecordDeserializer() {
        this(true);
    }

    public BinarySourceRecordDeserializer(boolean isCanal) {
        this.isCanal = isCanal;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<BinarySourceRecord> collector) throws Exception {
        if (RecordUtils.isSchemaChangeEvent(sourceRecord)) {
            return;
        }
        BinarySourceRecord record = BinarySourceRecord.builder(sourceRecord, this.isCanal);
        collector.collect(record);
    }

    @Override
    public TypeInformation<BinarySourceRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<BinarySourceRecord>() {
        });
    }
}
