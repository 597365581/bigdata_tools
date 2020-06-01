package com.yongqing.hive.tool.client;

import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 *
 */
public interface Client {
    void close();
    void reconnect() throws MetaException;
}
