package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;
import com.yongqing.log.flume.sink.log.process.EventToHiveByNoTransaction;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hdfs.tool.DefaultHdfsClient;
import com.yongqing.hive.tool.client.DefaultHiveMetaStoreClient;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class HiveByNoTransactionSink extends AbstractBigDataSink {
    private String etcdConfig;
    private static final Logger logger = LoggerFactory
            .getLogger(HiveByNoTransactionSink.class);


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
        if (StringUtils.isNotBlank(context.getString(SinkConstants.ETCD_CONFIG))) {
            this.etcdConfig = context.getString(SinkConstants.ETCD_CONFIG);
        }
        Preconditions.checkState(null != etcdConfig, etcdConfig
                + " must not be null");
        Preconditions.checkState(batchSize >= 1, batchSize
                + " must be >= 1");
    }

    @Override
    public void stop() {
        logger.info("HiveByNoTransactionSink sink {} stopping...");
        DefaultHiveMetaStoreClient.getHiveMetaStoreClient(EtcdUtil.getLocalPropertie("hiveMetaStoreUri")).close();
        try {
            DefaultHdfsClient.getHdfsClient().close();
        } catch (IOException e) {
            logger.error("hdfs client close cause error", e);
        }
        EtcdUtil.getEtclClient().close();
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public void start() {
        //初始化监听
        EtcdUtil.initListen(etcdConfig);
        eventTo = new EventToHiveByNoTransaction();
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("HiveByNoTransactionSink sink {} started...");
    }
}
