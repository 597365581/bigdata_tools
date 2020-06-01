package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;

import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.log.flume.sink.log.process.EventToMongoDb;
import com.yongqing.mongodb.client.MongodbClient;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.yongqing.log.flume.sink.SinkConstants.BATCH_SIZE;
import static com.yongqing.log.flume.sink.SinkConstants.ETCD_CONFIG;
import static com.yongqing.log.flume.sink.SinkConstants.LOG_TYPE;

/**
 *
 */
public class MongoDbSink extends AbstractBigDataSink {
    private String etcdConfig;
    private static final Logger logger = LoggerFactory
            .getLogger(MongoDbSink.class);



    @Override
    public long getBatchSize() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
        if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
            this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
        }
        //日志类型 1 系统请求日志  2 业务日志 3 Nginx日志
        if (StringUtils.isNotBlank(context.getString(LOG_TYPE))) {
            this.logType = context.getString(LOG_TYPE);
        }
        if (StringUtils.isNotBlank(context.getString(ETCD_CONFIG))) {
            this.etcdConfig = context.getString(ETCD_CONFIG);
        }
        Preconditions.checkState(null != etcdConfig, etcdConfig
                + " must not be null");
        Preconditions.checkState(batchSize >= 1, batchSize
                + " must be >= 1");
    }
    @Override
    public void stop() {
        logger.info("MongoDbSink sink {} stopping...");
        //关闭mongdb的链接
        MongodbClient.getInstance(EtcdUtil.getLocalPropertie("mongodbUrl")).close();
        EtcdUtil.getEtclClient().close();
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }
    @Override
    public void start() {
        //初始化监听
        EtcdUtil.initListen(etcdConfig);
        //init mongodb
        MongodbClient.getInstance(EtcdUtil.getLocalPropertie("mongodbUrl"));
        try {
            EtcdUtil.loadActionClass("com.yongqing.mongodb.client.MongodbClientEtcdAction");
        } catch (ClassNotFoundException e) {
            logger.error("loadActionClass cause Ecxeption",e);
        }
        eventTo = new EventToMongoDb();
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("MongoDbSink sink {} started...");
    }
}
