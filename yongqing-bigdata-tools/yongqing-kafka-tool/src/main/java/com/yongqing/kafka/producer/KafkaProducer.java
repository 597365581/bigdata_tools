package com.yongqing.kafka.producer;

import org.apache.kafka.clients.producer.Callback;

/**
 *
 */
public interface KafkaProducer {

    void sendMessage(String msg,Callback callback);
    void sendMessage(String msg,String topicName,Integer kafkaPartitionNumber);
    void sendMessage(String msg,String topicName,Integer kafkaPartitionNumber,Callback callback);
    void sendMessageWithCatch(String msg);
    void sendMessageWithCatch(String msg,Callback callback);
    void sendMessageWithCatch(String msg,String topicName,Integer kafkaPartitionNumber);
    void sendMessageWithCatch(String msg,String topicName,Integer kafkaPartitionNumber,Callback callback);
    void close();
    void flush();
    void close(long timeout, java.util.concurrent.TimeUnit timeUnit);
    void sendMessageWithTransactional(String msg);
    void sendMessageWithTransactional(String msg,String topicName,Integer kafkaPartitionNumber);
}
