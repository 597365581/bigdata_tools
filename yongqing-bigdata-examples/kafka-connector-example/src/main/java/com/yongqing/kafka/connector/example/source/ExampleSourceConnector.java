package com.yongqing.kafka.connector.example.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class ExampleSourceConnector extends SourceConnector{
    @Override
    public void start(Map<String, String> map) {

    }
    //返回需要指定的TASK
    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSourceTask.class;
    }
    //TASK的配置
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
