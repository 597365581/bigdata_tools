package com.yongqing.rocketmq.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 *  hbase sink的自定义实现
 */
public class HbaseSink extends AbstractSink implements Configurable, BatchSizeSupported {
    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public long getBatchSize() {
        return 0;
    }

    @Override
    public void configure(Context context) {

    }
}
