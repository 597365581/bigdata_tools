package com.yongqing.rocketmq.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.formatter.output.BucketPath;

/**
 * Created by zhangyongqing on 2019-10-15.
 */
public class DefaultIndexNameBuilder implements
        IndexNameBuilder {
    private String indexPrefix;
    @Override
    public String getIndexName(Event event) {
        String realIndexPrefix = BucketPath.escapeString(indexPrefix, event.getHeaders());
        return realIndexPrefix;
    }

    @Override
    public String getIndexPrefix(Event event) {
        return BucketPath.escapeString(indexPrefix, event.getHeaders());
    }

    @Override
    public void configure(Context context) {
        indexPrefix = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
