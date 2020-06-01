package com.yongqing.hive.tool.queue;

import com.yongqing.hive.tool.Event;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 *
 */
public class EventQueue {

    //阻塞队列
    private final static BlockingQueue<Event> eventArrayBlockingQueue = new LinkedBlockingQueue<Event>();



    public static BlockingQueue<Event> getEventQueue() {
        return eventArrayBlockingQueue;
    }
}
