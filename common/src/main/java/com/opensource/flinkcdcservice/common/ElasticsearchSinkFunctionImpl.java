package com.opensource.flinkcdcservice.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.opensource.flinkcdcservice.common.commons.ParamConstants;
import com.opensource.flinkcdcservice.common.record.BinarySourceRecord;
import com.opensource.flinkcdcservice.common.utils.ParameterWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.FlinkRuntimeException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

public class ElasticsearchSinkFunctionImpl implements ElasticsearchSinkFunction<BinarySourceRecord> {

    private final String docKey;
    private final String index;
    private final Elasticsearch7RequestFactory requestFactory;

    public ElasticsearchSinkFunctionImpl(ParameterWrapper parameterWrapper) {
        String sinkTopic = parameterWrapper.getString(ParamConstants.SINK_TOPIC);
        this.docKey = parameterWrapper.getString(ParamConstants.SINK_KEY);
        this.index = sinkTopic;
        this.requestFactory = new Elasticsearch7RequestFactory();
    }

    @Override
    public void process(BinarySourceRecord record, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        String indexStr = this.index != null ? this.index : record.getTopic();
        switch (record.getType().toUpperCase()) {
            case "INSERT":
                final IndexRequest request = this.requestFactory.createIndexRequest(indexStr,
                        generateKey(record.getData()), null, record.getData());
                requestIndexer.add(request);
                break;
            case "UPDATE":
                final UpdateRequest request1 = this.requestFactory.createUpdateRequest(indexStr,
                        generateKey(record.getData()), null, record.getData());
                requestIndexer.add(request1);
                break;
            case "DELETE":
                final DeleteRequest request2 = this.requestFactory.createDeleteRequest(indexStr, generateKey(record.getData()));
                requestIndexer.add(request2);
                break;
            default:
                throw new FlinkRuntimeException("Unsupported message kind " + record.getType());
        }
    }

    private String generateKey(JSONObject data) {
        if (data.containsKey(this.docKey)) {
            return data.getString(this.docKey);
        }
        throw new FlinkRuntimeException("This docKey is not exist in data");
    }

    static class Elasticsearch7RequestFactory implements RequestFactory<JSONObject> {

        @Override
        public UpdateRequest createUpdateRequest(String index, String key, XContentType contentType, JSONObject document) {
            String source = JSON.toJSONString(document, SerializerFeature.WriteMapNullValue);
            return new UpdateRequest(index, key).doc(source, XContentType.JSON).upsert(source, XContentType.JSON);
        }

        @Override
        public IndexRequest createIndexRequest(String index, String key, XContentType contentType, JSONObject document) {
            String source = JSON.toJSONString(document, SerializerFeature.WriteMapNullValue);
            return Requests.indexRequest().index(index).id(key).source(source, XContentType.JSON);
        }

        @Override
        public DeleteRequest createDeleteRequest(String index, String key) {
            return Requests.deleteRequest(index).id(key);
        }
    }
}
