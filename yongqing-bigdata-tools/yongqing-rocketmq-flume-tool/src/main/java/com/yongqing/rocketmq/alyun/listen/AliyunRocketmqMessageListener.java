package com.yongqing.rocketmq.alyun.listen;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.yongqing.rocketmq.queue.FlumeMq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AliyunRocketmqMessageListener implements MessageListener {
    public static Logger log = LoggerFactory.getLogger(AliyunRocketmqMessageListener.class);

    @Override
    public Action consume(Message message, ConsumeContext consumeContext) {
        log.info("Receive:{}",message);
        try {
            FlumeMq.getFlumeMq().put(message);
        } catch (InterruptedException e) {
             log.info("message put cause error",e);
        }
        return Action.CommitMessage;
    }
}
