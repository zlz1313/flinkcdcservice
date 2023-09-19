package com.opensource.flinkcdcservice.common;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;

public interface RequestFactory<T> extends Serializable {

    UpdateRequest createUpdateRequest(String index,String key, XContentType contentType, T var5);

    IndexRequest createIndexRequest(String index, String key, XContentType contentType, T var5);

    DeleteRequest createDeleteRequest(String index, String key);
}
