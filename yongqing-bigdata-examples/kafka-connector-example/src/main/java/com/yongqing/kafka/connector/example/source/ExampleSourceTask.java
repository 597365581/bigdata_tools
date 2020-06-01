package com.yongqing.kafka.connector.example.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class ExampleSourceTask extends SourceTask {
    @Override
    public String version() {
        return new ExampleSourceConnector().version();
    }
    //任务启动
    @Override
    public void start(Map<String, String> map) {

    }
    //需要发送到kafka的数据。
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }
    //任务停止
    @Override
    public void stop() {

    }
}
