package com.yongqing.hive.tool.constant;

/**
 *
 */
public interface Constants {
    int DEFAULT_MAXOPENCONNECTIONS = 500;
    int DEFAULT_TXNSPERBATCH = 100;
    int DEFAULT_BATCHSIZE = 15;
    int DEFAULT_CALLTIMEOUT = 10000;
    int DEFAULT_IDLETIMEOUT = 0;
    int DEFAULT_HEARTBEATINTERVAL = 240; // seconds
    String HIVEMETASTOREURIS = "hive.metastore.uris";
}
