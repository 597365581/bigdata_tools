package com.yongqing.log.flume.sink;

import com.google.common.base.Preconditions;
import com.yongqing.log.flume.sink.log.process.EventToKafka;
import com.yongqing.kafka.producer.MyKafkaProducer;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.yongqing.log.flume.sink.SinkConstants.*;

/**
 *
 */
public class KafkaSink extends AbstractBigDataSink {
    private String kafkaTopic;
    private Integer kafkaPartitionNumber;
    private static final Logger logger = LoggerFactory
            .getLogger(KafkaSink.class);
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
        if (StringUtils.isNotBlank(context.getString(KAFKA_TOPIC))) {
            this.kafkaTopic = context.getString(KAFKA_TOPIC);
        }
        if (StringUtils.isNotBlank(context.getString(KAFKA_PARTITION_NUMBER))) {
            this.kafkaPartitionNumber = Integer.valueOf(context.getString(KAFKA_PARTITION_NUMBER));
        }
        Preconditions.checkState(null != kafkaTopic, kafkaTopic
                + " must not be null");
        Preconditions.checkState(kafkaPartitionNumber >= 0, kafkaPartitionNumber
                + " must be >= 0");
        Preconditions.checkState(batchSize >= 1, batchSize
                + " must be >= 1");
    }
    @Override
    public void stop() {
        logger.info("Kafka sink {} stopping");
        //关闭kafka的链接
        MyKafkaProducer.getMyKafkaProducerInstance().close();
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }
    @Override
    public void start() {
        //init kafka
        MyKafkaProducer.getMyKafkaProducerInstance();
        eventTo = new EventToKafka(kafkaTopic, kafkaPartitionNumber);
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
        logger.info("Kafka sink {} started");
    }
}
