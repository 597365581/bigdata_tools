package com.yongqing.hive.tool.pojo;

import com.yongqing.hive.tool.Event;

import java.util.Map;

/**
 *
 */
public class DefaultEvent implements Event {
    private Map<String, String> headers;
    private byte[] body;

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public void setBody(byte[] body) {
        this.body = body;
    }
}
