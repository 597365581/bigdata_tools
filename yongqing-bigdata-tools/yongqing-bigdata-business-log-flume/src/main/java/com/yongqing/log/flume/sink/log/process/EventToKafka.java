package com.yongqing.log.flume.sink.log.process;

import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.kafka.producer.MyKafkaProducer;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public class EventToKafka implements EventTo {
    private static final Logger logger = LoggerFactory
            .getLogger(EventToKafka.class);
    private List<String> bulkBuilder;
    private String kafkaTopic;
    private Integer kafkaPartitionNumber;

    public EventToKafka() {
        bulkBuilder = new ArrayList<String>();
        this.kafkaTopic = EtcdUtil.getLocalPropertie("kafkaTopic");
        this.kafkaPartitionNumber = Integer.valueOf(EtcdUtil.getLocalPropertie("kafkaPartitionNumber"));
    }

    public EventToKafka(List<String> bulkBuilder, String kafkaTopic, Integer kafkaPartitionNumber) {
        this.bulkBuilder = bulkBuilder;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartitionNumber = kafkaPartitionNumber;
    }

    public EventToKafka(String kafkaTopic, Integer kafkaPartitionNumber) {
        bulkBuilder = new ArrayList<String>();
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartitionNumber = kafkaPartitionNumber;
    }

    public void addEvent(Event event, String logType) {
        //暂时只处理业务日志和爬虫数据以及nginx 日志
        if ("2".equals(logType) || "5".equals(logType) ||"3".equals(logType)) {
            synchronized (bulkBuilder) {
                try {
                    bulkBuilder.add(new String(event.getBody(), "utf-8"));
                } catch (UnsupportedEncodingException e) {
                    logger.error("addEvent cause Exception",e);
                }
            }
        }
    }

    public void execute(String logType) {
        List<String> logs;
        synchronized (bulkBuilder) {
            logs = bulkBuilder;
            //发送到kafka中去
            logs.forEach(log -> {
                if (null != log && log.contains("collectionSign")) {
                    if ("2".equals(logType) || "5".equals(logType)) {
                        if (log.split("- \\{").length >= 2) {
                            MyKafkaProducer.getMyKafkaProducerInstance().sendMessage("{" + (log.split("- \\{"))[1], kafkaTopic, kafkaPartitionNumber);
                        } else if (log.startsWith("{") && log.endsWith("}")) {
                            MyKafkaProducer.getMyKafkaProducerInstance().sendMessage(log, kafkaTopic, kafkaPartitionNumber);
                        }
                        else {
                            logger.info("log:"+log+" can not to process...");
                        }
                    }
                    // nginx log
                    else if("3".equals(logType)){
                        MyKafkaProducer.getMyKafkaProducerInstance().sendMessage(log, kafkaTopic, kafkaPartitionNumber);
                    }
                    else {
                        logger.error("unknow logType:{}", logType);
                    }
                } else {
                    logger.error("send log:{} msg error", log);
                }
            });
            bulkBuilder.clear();
        }
    }
}
