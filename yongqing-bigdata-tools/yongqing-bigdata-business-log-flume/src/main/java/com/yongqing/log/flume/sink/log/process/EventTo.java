package com.yongqing.log.flume.sink.log.process;

import org.apache.flume.Event;

/**
 *
 */
public interface EventTo {
    void addEvent(Event event, String logType);
    void execute(String logType);
}
