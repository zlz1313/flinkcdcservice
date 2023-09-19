package com.opensource.flinkcdcservice.common;

import com.opensource.flinkcdcservice.common.deserializer.BinarySourceRecordDeserializer;
import com.opensource.flinkcdcservice.common.env.EnvWrapper;
import com.opensource.flinkcdcservice.common.record.BinarySourceRecord;
import com.opensource.flinkcdcservice.common.utils.ParameterWrapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Properties;

public class MySQL2ESCdcService {

    public static void main(String[] args) throws Exception {
        ParameterWrapper param = new ParameterWrapper(args);

        EnvWrapper envWrapper = new EnvWrapper();
        envWrapper.setGlobalConfig(param);

        Properties properties = new Properties();
        properties.put("converters", "mysqltimezoneconverter");
        properties.put("mysqltimezoneconverter.type", "com.opensource.flinkcdcservice.common.converter.MySQLDateTimeConverter");
        MySQLSourceWrapper<BinarySourceRecord> mySQLSource = new MySQLSourceWrapper<>(param);
        mySQLSource.setDeserializer(new BinarySourceRecordDeserializer(), properties);

        DataStreamSource<BinarySourceRecord> dataStream = envWrapper.buildSource(
                mySQLSource.build(), mySQLSource.getSourceName());
        envWrapper.buildSink(dataStream, EsSinkProducer.getSink(param), "Elasticsearch Sink");
        envWrapper.executeJob("MySQL2Es-" + mySQLSource.getJobName());
    }
}
