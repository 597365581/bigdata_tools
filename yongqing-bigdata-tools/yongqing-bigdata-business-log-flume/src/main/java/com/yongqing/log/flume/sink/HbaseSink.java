package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;
import com.yongqing.log.flume.sink.log.process.EventToHbase;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hbase.utils.HbaseClient;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HbaseSink extends AbstractBigDataSink {
    private String etcdConfig;
    private static final Logger logger = LoggerFactory
            .getLogger(HbaseSink.class);
    @Override
    public long getBatchSize() {
        return 0;
    }
    @Override
    public void configure(Context context) {
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
        if (StringUtils.isNotBlank(context.getString(SinkConstants.BATCH_SIZE))) {
            this.batchSize = Integer.parseInt(context.getString(SinkConstants.BATCH_SIZE));
        }
        //日志类型 1 系统请求日志  2 业务日志 3 Nginx日志
        if (StringUtils.isNotBlank(context.getString(SinkConstants.LOG_TYPE))) {
            this.logType = context.getString(SinkConstants.LOG_TYPE);
        }
        Preconditions.checkState(batchSize >= 1, SinkConstants.BATCH_SIZE
                + " must be greater than 0");
        if (StringUtils.isNotBlank(context.getString(SinkConstants.ETCD_CONFIG))) {
            this.etcdConfig = context.getString(SinkConstants.ETCD_CONFIG);
        }
        Preconditions.checkState(null != etcdConfig, etcdConfig
                + " must not be null");
    }
    @Override
    public void stop() {
        logger.info("Hbase sink {} stopping");
        //关闭hbase的链接
        HbaseClient.getInstance(EtcdUtil.getLocalPropertie("zkClient"),EtcdUtil.getLocalPropertie("zkClientPort")).close();
        EtcdUtil.getEtclClient().close();
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }
    @Override
    public void start() {
        //初始化监听
        EtcdUtil.initListen(etcdConfig);
        //init hbase
        HbaseClient.getInstance(EtcdUtil.getLocalPropertie("zkClient"),EtcdUtil.getLocalPropertie("zkClientPort"));
        eventTo = new EventToHbase();
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("Hbase sink {} started");
    }
}