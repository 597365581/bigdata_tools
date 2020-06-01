package com.yongqing.rocketmq.elasticsearch;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 *
 */
public interface ElasticSearchEventSerializer extends Configurable, ConfigurableComponent {

    Charset charset = Charset.defaultCharset();
    XContentBuilder getContentBuilder(Event event) throws IOException;
}
