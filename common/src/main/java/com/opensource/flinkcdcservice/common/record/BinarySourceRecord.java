package com.opensource.flinkcdcservice.common.record;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import io.debezium.data.Envelope;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 主要是关系型源数据
 */
@Data
public class BinarySourceRecord {
    private boolean isDdl;
    private String sql;
    private String db;
    private String table;
    private String type;
    private long ts;
    private long es;
    private String topic;

    private JSONObject data;
    private JSONObject old;

    private List<String> pkNames;

    public static BinarySourceRecord builder(SourceRecord sourceRecord, boolean isCanal) {
        BinarySourceRecord record = new BinarySourceRecord();
        record.fromSourceRecord(sourceRecord, isCanal);
        return record;
    }

    private BinarySourceRecord fromSourceRecord(SourceRecord sourceRecord, boolean isCanal) {
        this.sql = "";
        this.isDdl = false;
        this.topic = sourceRecord.topic();

        Struct value = (Struct)sourceRecord.value();
        Field srcField = value.schema().field(Envelope.FieldName.SOURCE);
        if (srcField != null) {
            Struct srcStruct  = value.getStruct(srcField.name());
            this.db = srcStruct.getString("db");
            this.table = srcStruct.getString("table");
            this.es = srcStruct.getInt64("ts_ms");
        }
        Field tsField = value.schema().field(Envelope.FieldName.TIMESTAMP);
        if (tsField != null) {
            this.ts = value.getInt64(tsField.name());
        }
        this.type = getOperation(sourceRecord);
        if (isCanal) {
            JSONObject  before = getRecordImageForCanal(value, "before");
            JSONObject  after = getRecordImageForCanal(value, "after");
            this.old = Envelope.Operation.DELETE.name().equals(this.type) ? after : before;
            this.data = Envelope.Operation.DELETE.name().equals(this.type) ? before : after;
        } else {
            this.old = getRecordImage(value, "before");
            this.data = getRecordImage(value, "after");
        }
        Schema keySchema = sourceRecord.keySchema();
        if (keySchema != null && keySchema.fields() != null) {
            this.pkNames = keySchema.fields().stream().map(Field::name).collect(Collectors.toList());
        }
        return this;
    }

    /**
     * 兼容canal的JSON格式
     */
    @Override
    public String toString() {
        JSONObject result = new JSONObject();
        result.put("isDdl", this.isDdl);
        result.put("sql", this.sql);
        result.put("database", this.db);
        result.put("table", this.table);
        result.put("ts", this.ts);
        result.put("es", this.es);
        result.put("pkNames", this.pkNames);
        result.put("type", this.type);
        result.put("topic", this.topic);
        result.put("old", this.old.isEmpty() ? null : Collections.singletonList(this.old));
        result.put("data", this.data.isEmpty() ? null : Collections.singletonList(this.data));
        return JSON.toJSONString(result, SerializerFeature.WriteMapNullValue);
    }

    private static String getOperation(SourceRecord sourceRecord) {
        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        if (Envelope.Operation.CREATE.equals(op)) {
            return "INSERT";
        } else if (Envelope.Operation.READ.equals(op)) {
            return "INSERT";
        } else {
            return op.name();
        }
    }

    private static JSONObject getRecordImageForCanal(Struct value, String type) {
        Struct image = value.getStruct(type);
        JSONObject imageJson = new JSONObject();
        if (image != null) {
            Schema schema = image.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object fieldVal = image.get(field);
                imageJson.put(field.name(), fieldVal == null ? null : String.valueOf(fieldVal));
            }
        }
        return imageJson;
    }

    private static JSONObject getRecordImage(Struct value, String type) {
        Struct image = value.getStruct(type);
        JSONObject imageJson = new JSONObject();
        if (image != null) {
            Schema schema = image.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object fieldVal = image.get(field);
                imageJson.put(field.name(), fieldVal);
            }
        }
        return imageJson;
    }
}
