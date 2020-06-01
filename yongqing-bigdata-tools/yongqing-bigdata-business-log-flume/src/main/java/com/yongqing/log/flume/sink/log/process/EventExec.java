package com.yongqing.log.flume.sink.log.process;

import java.util.Map;

/**
 *
 */
@FunctionalInterface
public interface EventExec {
    void exec(String docId, Map<String, Object> customizeField, String dataBaseName, String tableName);
}
