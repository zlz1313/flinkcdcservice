package com.opensource.flinkcdcservice.common;

import com.opensource.flinkcdcservice.common.commons.ParamConstants;
import com.opensource.flinkcdcservice.common.record.BinarySourceRecord;
import com.opensource.flinkcdcservice.common.utils.ParameterWrapper;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

public class EsSinkProducer {
    public static ElasticsearchSink<BinarySourceRecord> getSink(ParameterWrapper parameters) {
        String host = parameters.getString(ParamConstants.SINK_HOST);
        ElasticsearchSink.Builder<BinarySourceRecord> builder = new ElasticsearchSink.Builder<>(create(host),
                new ElasticsearchSinkFunctionImpl(parameters));

        // 通过实现AuthRest 支持es的认证功能
        String username = parameters.getString(ParamConstants.SINK_USER);
        String password = parameters.getString(ParamConstants.SINK_PWD);
        AuthRestClientFactory authRestClientFactory = new AuthRestClientFactory(null, username, password);
        builder.setRestClientFactory(authRestClientFactory);

        // 设置sink commit频率
        builder.setBulkFlushMaxActions(64);
        builder.setBulkFlushInterval(1000);
        return builder.build();
    }

    public static List<HttpHost> create(String hostStr) {
        List<HttpHost> httpHosts = new ArrayList<>();
        String[] hosts = hostStr.split(",");
        for (String host : hosts) {
            httpHosts.add(HttpHost.create(host));
        }
        return httpHosts;
    }


}
