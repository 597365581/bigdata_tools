package com.yongqing.log.flume.sink;


import com.google.common.base.Throwables;
import com.google.gson.reflect.TypeToken;
import com.yongqing.log.flume.sink.log.process.EventTo;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.crawler.analysis.TaxCrawlerAnalysis;
import com.yongqing.crawler.analysis.tax.bean.TaxCrawlerAnalysisBean;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.processor.log.Processor;
import com.yongqing.processor.log.bean.ProcessorBean;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.yongqing.crawler.analysis.constants.TaxCrawlerConstant.taxCrawlerAnalysisBeanList;
/**
 *
 */
public abstract class AbstractBigDataSink extends AbstractSink implements org.apache.flume.conf.Configurable, BatchSizeSupported {
    protected final CounterGroup counterGroup = new CounterGroup();
    protected static final int defaultBatchSize = 5;
    protected int batchSize = defaultBatchSize;
    protected static final String defaultLogType = "1";
    protected String logType = defaultLogType;
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractBigDataSink.class);
    protected SinkCounter sinkCounter;
    protected EventTo eventTo;

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("processing...");
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        try {
            txn.begin();
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    logger.info("Sink receive event is null");
                    break;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("receive log event:{}", new String(event.getBody()));
                }
                logger.info("Sink receive one event,event size is:{}", event.getBody().length);
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
            txn.commit();
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
            txn.close();
        }
        return status;
    }

    @Override
    public long getBatchSize() {
        return 0;
    }

    public void start() {
        if (null != EtcdUtil.getLocalPropertie("processors")) {
            synchronized (AbstractBigDataSink.class) {
                SinkConstants.processorBeanList = GsonUtil.gson.fromJson(EtcdUtil.getLocalPropertie("processors"), new TypeToken<List<ProcessorBean>>() {
                }.getType());
                SinkConstants.processorBeanList.forEach(processorBean -> {
                    try {
                        Processor<?> processor = (Processor<?>) Class.forName(processorBean.getProcessor()).newInstance();
                        processorBean.setProcessorInstance(processor);
                    } catch (Throwable e) {
                        logger.error("get processor instance cause Exeption", e);
                    }
                });
            }
        }
        if (null != EtcdUtil.getLocalPropertie("taxCrawlerAnalysises")) {
            synchronized (AbstractBigDataSink.class) {
                taxCrawlerAnalysisBeanList = GsonUtil.gson.fromJson(EtcdUtil.getLocalPropertie("taxCrawlerAnalysises"), new TypeToken<List<TaxCrawlerAnalysisBean>>() {
                }.getType());
                taxCrawlerAnalysisBeanList.forEach(taxCrawlerAnalysisBean -> {
                    try {
                        TaxCrawlerAnalysis taxCrawlerAnalysis = (TaxCrawlerAnalysis) Class.forName(taxCrawlerAnalysisBean.getTaxCrawlerAnalysis()).newInstance();
                        taxCrawlerAnalysisBean.setTaxCrawlerAnalysisInstance(taxCrawlerAnalysis);
                    } catch (Throwable e) {
                        logger.error("get taxCrawlerAnalysises instance cause Exeption", e);
                    }
                });
            }
        }
        super.start();
    }

}
