package com.yongqing.hbase.exception;

/**
 * 自定义的hbase异常类
 */
public class HbaseException extends RuntimeException {
    public HbaseException(String s, Exception e) {
        super(s, e);
    }
    public HbaseException(String s){
        super(s);
    }
}
