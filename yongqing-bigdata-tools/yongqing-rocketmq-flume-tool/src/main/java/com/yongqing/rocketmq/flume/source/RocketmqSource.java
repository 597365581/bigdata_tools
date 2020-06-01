package com.yongqing.rocketmq.flume.source;

import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.yongqing.rocketmq.alyun.listen.AliyunRocketmqMessageListener;
import com.yongqing.rocketmq.queue.FlumeMq;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;


/**
 * rocketmq 的flume source
 */
public class RocketmqSource extends AbstractSource implements Configurable, PollableSource {
    public static Logger log = LoggerFactory.getLogger(AliyunRocketmqMessageListener.class);
    private String accessKey;

    private String secretKey;

    private String namesrvAddr;

    private String groupId;

    private Consumer consumer;

    private String topic;

    private String instanceId;

    @Override
    public Status process() throws EventDeliveryException {
        // 创建事件头信息
        HashMap<String, String> hearderMap = new HashMap<>();
        hearderMap.put("topic", topic);
        hearderMap.put("groupId", groupId);
        //事件
        Event event = new SimpleEvent();
        try {
            event.setBody(FlumeMq.getFlumeMq().take().getBody());
            event.setHeaders(hearderMap);
        } catch (InterruptedException e) {
            log.error("flume get message cause error", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("flume source messge:{},topic:{},groupId:{}", new String(event.getBody()), event.getHeaders().get("topic"), event.getHeaders().get("groupId"));
        }
        // 将事件写入 channel
        getChannelProcessor().processEvent(event);

        log.info("put one event success...");
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        //获取rocketmq的配置
        accessKey = context.getString("aliyunMqAccessKey");
        secretKey = context.getString("aliyunMqSecretKey");
        namesrvAddr = context.getString("aliyunMqNamesrvAddr");
        groupId = context.getString("groupId");
        topic = context.getString("topic");
        instanceId = context.getString("instanceId");
        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey) || StringUtils.isBlank(namesrvAddr) || StringUtils.isBlank(groupId) || StringUtils.isBlank(topic) || StringUtils.isBlank(instanceId)) {
            log.error("Please check flume source config,accessKey or secretKey or namesrvAddr or groupId or topic or instanceId is null... ");
        }
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.GROUP_ID, groupId);
        properties.put(PropertyKeyConst.INSTANCE_ID, instanceId);
        properties.put(PropertyKeyConst.AccessKey, accessKey);
        properties.put(PropertyKeyConst.SecretKey, secretKey);
        properties.put(PropertyKeyConst.NAMESRV_ADDR, namesrvAddr);
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        consumer = ONSFactory.createConsumer(properties);
    }

    @Override
    public void start() {
        consumer.subscribe(topic, "*", new AliyunRocketmqMessageListener());
        consumer.start();
        super.start();
    }
}
