package com.yongqing.hive.tool;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;

/**
 * 自定义的hive异常类
 */
public class HiveException extends RuntimeException {
    public HiveException(String s, Exception e) {
        super(s, e);
    }
    public HiveException(String s){
        super(s);
    }

    public HiveException(HiveEndPoint endPoint, Long currentTxnId, Throwable e) {
        super("Failed writing to : " + endPoint + ". TxnID : " + currentTxnId, e);
    }
}
