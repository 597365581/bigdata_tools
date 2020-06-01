package com.yongqing.kafka.producer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 */
public class MyKafkaProducer extends KafkaAbstractProducer{
    //log
    private static final Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);
    private MyKafkaProducer(){
        super();
    }
    // 实例
    private static final MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
    public synchronized static MyKafkaProducer getMyKafkaProducerInstance(){
        if(null == kafkaProducer){
            myKafkaProducer.initKafkaProducer(null);
        }
        if(null == kafkaProducer){
            logger.error("initKafkaProducer cause error");
        }
        else {
            logger.info("initKafkaProducer success");
        }
        return myKafkaProducer;
    }
    public synchronized static MyKafkaProducer getMyKafkaProducerInstance(Properties properties){
        if(null == kafkaProducer){
            kafkaProducer = new KafkaProducer<Object,Object>(properties);
            topic=properties.getProperty("topicName");
            kafkaPartitions=Integer.valueOf(properties.getProperty("kafkaPartitionNumber"));
        }
        if(null == kafkaProducer){
            logger.error("initKafkaProducer cause error");
        }
        else {
            logger.info("initKafkaProducer success");
        }
        return myKafkaProducer;
    }
    public synchronized static MyKafkaProducer getMyKafkaProducerInstance(String bootstrapServers){
        if(null == kafkaProducer){
            myKafkaProducer.initKafkaProducer(bootstrapServers);
        }
        if(null == kafkaProducer){
            logger.error("initKafkaProducer cause error");
        }
        else {
            logger.info("initKafkaProducer success");
        }
        return myKafkaProducer;
    }

    private Producer<Object,Object> initKafkaProducer(String bootstrapServers)  {
        if(null != kafkaProducer){
            return kafkaProducer;
        }
        InputStream is = MyKafkaProducer.class.getClassLoader().getResourceAsStream("kafka.properties");
        //获取kafka的的相关配置
        //TODO 待继续完成
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            logger.error("initKafkaProducer cause error",e);
        }
        finally {
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("close kafka.properties fail", e);
                }
            }
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers",null==bootstrapServers?prop.getProperty("bootstrap.servers"):bootstrapServers);
        properties.put("acks", "all");
        properties.put("retries", 2);
        properties.put("max.request.size", 5242880);
        properties.put("batch.size",prop.getProperty("batch.size"));
        properties.put("linger.ms", prop.getProperty("linger.ms"));
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        properties.put("serializer.class","kafka.serializer.StringEncoder");
//        properties.put("partitioner.class", KafkaPartitioner.class.getName());
//        properties.put("metadata.broker.list", "");
//        properties.put("producer.type", "async");
//        properties.put("queue.buffering.max.ms", "");
//        properties.put("queue.buffering.max.messages", "");
        kafkaProducer = new KafkaProducer<Object,Object>(properties);
        topic=prop.getProperty("topicName");
        kafkaPartitions=Integer.valueOf(prop.getProperty("kafkaPartitionNumber"));
        return kafkaProducer;
    }
}
