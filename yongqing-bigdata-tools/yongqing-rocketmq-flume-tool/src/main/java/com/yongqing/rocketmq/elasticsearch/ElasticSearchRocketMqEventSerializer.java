package com.yongqing.rocketmq.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
/**
 *  RocketMq消息的Serializer
 */
public class ElasticSearchRocketMqEventSerializer implements ElasticSearchEventSerializer {
    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendHeaders(builder,event);
        builder.endObject();
        return builder;
    }
    private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
        ContentBuilderUtil.appendField(builder,"topic",event.getHeaders().get("topic").getBytes(charset));
        ContentBuilderUtil.appendField(builder,"groupId",event.getHeaders().get("groupId").getBytes(charset));
        ContentBuilderUtil.appendField(builder,"message",event.getBody());
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
