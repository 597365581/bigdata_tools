package com.yongqing.rocketmq.elasticsearch.client;

import com.yongqing.rocketmq.elasticsearch.IndexNameBuilder;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

/**
 *
 */
public interface Client extends Configurable {

    void close();

    void addEvent(Event event, IndexNameBuilder indexNameBuilder,
                         String indexType, long ttlMs) throws Exception;

    void execute() throws Exception;
}
