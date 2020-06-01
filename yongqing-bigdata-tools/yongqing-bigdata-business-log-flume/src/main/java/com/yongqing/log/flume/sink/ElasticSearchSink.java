package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;
import com.yongqing.elasticsearch.client.DefaultSingletonElasticSearchClient;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.log.flume.sink.log.process.EventToEs;
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
public class ElasticSearchSink extends AbstractBigDataSink {

    private String etcdConfig;
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchSink.class);


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
        logger.info("ElasticSearch sink {} stopping");

        //关闭es的链接
        try {
            DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(",")).close();
        } catch (Exception e) {
            e.printStackTrace();
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
        //init es
        DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(","));
        eventTo = new EventToEs();
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("ElasticSearch sink {} started");
    }
}
