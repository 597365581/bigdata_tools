package com.yongqing.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;

/**
 *
 */
public abstract class KafkaAbstractProducer implements KafkaProducer{
    //log
    private static final Logger logger = LoggerFactory.getLogger(KafkaAbstractProducer.class);
    //Kafaka消息生产者
    public static volatile Producer<Object,Object> kafkaProducer = null;
    public static Integer kafkaPartitions;
    public static String topic;
    public void sendMessage(String msg){
        sendMessage(msg,topic,kafkaPartitions);
    }
    public void sendMessage(String msg,Callback callback){
        sendMessage(msg,topic,kafkaPartitions,callback);
    }
    public void sendMessage(String msg,String topicName,Integer kafkaPartitionNumber){
        if(null == msg){
            return;
        }
        //发送消息
        logger.info("start send kafka msg:" + msg);
        kafkaProducer.send(getProducerRecord(msg,topicName,kafkaPartitionNumber));
        logger.info("send kafka msg success!");
    }
    public void sendMessage(String msg,String topicName,Integer kafkaPartitionNumber,Callback callback){
        if(null == msg){
            return;
        }
        //发送消息
        logger.info("start send kafka msg:" + msg);
        kafkaProducer.send(getProducerRecord(msg,topicName,kafkaPartitionNumber),callback);
        logger.info("send kafka msg success!");
    }
    public void sendMessageWithCatch(String msg) {
        try {
            sendMessage(msg);
        } catch (Throwable e) {
            logger.error("sendMessageToKfkaWithCatch error msg:" + msg, e);
        }
    }
    public void sendMessageWithCatch(String msg,Callback callback) {
        try {
            sendMessage(msg,callback);
        } catch (Throwable e) {
            logger.error("sendMessageToKfkaWithCatch error msg:" + msg, e);
        }
    }
    public void sendMessageWithCatch(String msg,String topicName,Integer kafkaPartitionNumber) {
        try {
            sendMessage(msg,topicName,kafkaPartitionNumber);
        } catch (Throwable e) {
            logger.error("sendMessageToKfkaWithCatch error msg:" + msg, e);
        }
    }
    public void sendMessageWithCatch(String msg,String topicName,Integer kafkaPartitionNumber,Callback callback) {
        try {
            sendMessage(msg,topicName,kafkaPartitionNumber,callback);
        } catch (Throwable e) {
            logger.error("sendMessageToKfkaWithCatch error msg:" + msg, e);
        }
    }
    public void close(){
        if(null != kafkaProducer){
            kafkaProducer.close();
        }
    }
    public void flush(){
        if(null != kafkaProducer){
            kafkaProducer.flush();
        }
    }
    public void close(long timeout, java.util.concurrent.TimeUnit timeUnit){
        if(null != kafkaProducer){
            kafkaProducer.close(timeout,timeUnit);
        }
    }
    public void sendMessageWithTransactional(String msg){
        sendMessageWithTransactional(msg,topic,kafkaPartitions);
    }
    public void sendMessageWithTransactional(String msg,String topicName,Integer kafkaPartitionNumber){
        if(null == msg){
            return;
        }
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        //发送消息
        logger.info("start send kafka msg with Transactional:" + msg);
        kafkaProducer.send(getProducerRecord(msg,topicName,kafkaPartitionNumber));
        kafkaProducer.commitTransaction();
        logger.info("send kafka msg with Transactional success!");
    }
    private ProducerRecord<Object, Object> getProducerRecord(String msg,String topicName,Integer kafkaPartitionNumber){
        //根所分区总数，随机选取分取存数据
        Integer randomPartition = new Random().nextInt(kafkaPartitionNumber);
        //生成kafka消息对象
        ProducerRecord<Object,Object> message = new ProducerRecord<Object, Object>(topicName, randomPartition,String.valueOf(randomPartition), msg);
        return message;
    }
    public Producer<Object,Object> getKafkaProducer(){
        return kafkaProducer;
    }
}
