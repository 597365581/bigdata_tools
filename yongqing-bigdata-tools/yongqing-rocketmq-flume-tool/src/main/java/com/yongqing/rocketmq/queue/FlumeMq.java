package com.yongqing.rocketmq.queue;

import com.aliyun.openservices.ons.api.Message;

import java.util.concurrent.SynchronousQueue;

/**
 *
 */
public class FlumeMq {
    //同步阻塞队列
    private final static SynchronousQueue<Message> flumeMq = new SynchronousQueue<Message>();

    public static SynchronousQueue<Message> getFlumeMq() {
        return flumeMq;
    }
}
