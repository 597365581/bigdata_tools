package com.yongqing.kafka.connector.example.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class ExampleSinkTask extends SinkTask {
    @Override
    public String version() {
        return new ExampleSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public void put(Collection<SinkRecord> collection) {

    }
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets){

    }
    @Override
    public void stop() {

    }
}
