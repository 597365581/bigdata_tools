package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.reflect.TypeToken;
import com.yongqing.log.flume.sink.bean.DataBaseConfig;
import com.yongqing.log.flume.sink.log.process.EventToHive;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hive.tool.HiveJsonDataSerializerImpl;
import com.yongqing.hive.tool.HiveWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.yongqing.common.bigdata.tool.GsonUtil.gson;

/**
 *
 */
public class HiveSink extends AbstractBigDataSink {

    private String etcdConfig;
    private static final Logger logger = LoggerFactory
            .getLogger(HiveSink.class);

    private Integer heartBeatInterval = 240;
    private ExecutorService callTimeoutPool;
    private Timer heartBeatTimer = new Timer();
   // private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);

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
        logger.info("hive sink {} stopping");
        if (null != EventToHive.hiveWriterList && EventToHive.hiveWriterList.size() > 0) {
            for (HiveWriter hiveWriter : EventToHive.hiveWriterList) {
                try {
                    hiveWriter.close();
                } catch (InterruptedException e) {
                    logger.error("hiveWriter.close() cause Exception", e);
                }
            }
        }
        EtcdUtil.getEtclClient().close();

        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    public Status process() throws EventDeliveryException {
        logger.debug("processing...");
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        boolean success = false;
        try {
            txn.begin();
            if (null!=EtcdUtil.getLocalPropertie("timeToSendHeartBeat") && "1".equals(EtcdUtil.getLocalPropertie("timeToSendHeartBeat"))) {
                enableHeartBeatOnAllWriters();
            }
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    logger.info("Hive Sink receive event is null");
                    break;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("receive log event:{}", new String(event.getBody()));
                }
                logger.info("Hive Sink receive one event,event size is:{}", event.getBody().length);
                eventTo.addEvent(event, logType);
            }
            //处理Event数据
            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(count);
                eventTo.execute(logType);
            }

            if (null != EventToHive.hiveWriterList && EventToHive.hiveWriterList.size() > 0) {
                for (HiveWriter hiveWriter : EventToHive.hiveWriterList) {
                    hiveWriter.flush(true);
                }
            }
            txn.commit();
            success = true;
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable e) {
            try {
                logger.error("Sink cause exception", e);
                txn.rollback();
                counterGroup.incrementAndGet("transaction.rollback");
            } catch (Exception ex2) {
                ex2.printStackTrace();
                logger.error(
                        "Exception in rollback. Rollback might not have been successful.",
                        ex2);
            }
            if (e instanceof Error || e instanceof RuntimeException) {
                logger.error("Failed to commit transaction. Transaction rolled back...",
                        e);
                Throwables.propagate(e);
            } else {
                logger.error("Failed to  commit transaction. Transaction rolled back...",
                        e);
                throw new EventDeliveryException(
                        "Failed to commit transaction. Transaction rolled back...", e);
            }
        } finally {
            if (!success) {
                txn.rollback();
            }
            txn.close();
        }
        return status;
    }

    @Override
    public void start() {
        //初始化监听
        EtcdUtil.initListen(etcdConfig);
        EventToHive.hiveWriterList = new ArrayList<>();
        callTimeoutPool = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("hive sink").build());
        List<DataBaseConfig> dataBaseConfigList = gson.fromJson(EtcdUtil.getLocalPropertie("dataBaseConfig"), new TypeToken<List<DataBaseConfig>>() {
        }.getType());
        for (DataBaseConfig dataBaseConfig : dataBaseConfigList) {
            HiveWriter hiveWriter = null;
            try {
                hiveWriter = new HiveWriter(new org.apache.hive.hcatalog.streaming.HiveEndPoint(EtcdUtil.getLocalPropertie("hiveMetaStoreUri"), dataBaseConfig.getDatabaseName(), dataBaseConfig.getTableName(), null), 100, false, 10000, callTimeoutPool, EtcdUtil.getLocalPropertie("hiveUser"), new HiveJsonDataSerializerImpl());

            } catch (HiveWriter.ConnectException e) {
                logger.error("HiveWriter create cause Exception", e);
            } catch (InterruptedException e) {
                logger.error("HiveWriter create cause Exception", e);
            }
            EventToHive.hiveWriterList.add(hiveWriter);
        }
        setupHeartBeatTimer();

        eventTo = new EventToHive();
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("hive sink {} started");
    }

    private void setupHeartBeatTimer() {
        if (heartBeatInterval > 0) {
            heartBeatTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    setupHeartBeatTimer();
                }
            }, heartBeatInterval * 1000);
        }
    }

    private void enableHeartBeatOnAllWriters() {
        for (HiveWriter writer : EventToHive.hiveWriterList) {
            writer.setHearbeatNeeded();
        }
    }
}
