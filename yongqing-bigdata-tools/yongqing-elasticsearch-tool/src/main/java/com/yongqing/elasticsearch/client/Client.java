package com.yongqing.elasticsearch.client;

import org.elasticsearch.client.RestHighLevelClient;

/**
 *
 */
public interface Client {
    RestHighLevelClient getClient();
    void close();
}
