package com.yongqing.elasticsearch.client.exception;

/**
 * 自定义的es异常类
 */
public class ElasticsearchException extends RuntimeException {
    public ElasticsearchException(String s, Exception e) {
        super(s, e);
    }
    public ElasticsearchException(String s){
        super(s);
    }
}
