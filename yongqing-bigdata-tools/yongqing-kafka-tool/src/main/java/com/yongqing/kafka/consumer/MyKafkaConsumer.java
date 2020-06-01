package com.yongqing.kafka.consumer;
import com.yongqing.kafka.producer.MyKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 *
 */
public class MyKafkaConsumer {
    //log
    private static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);
    //Kafaka消息消费者
    public static volatile KafkaConsumer<String, String> consumer = null;

    private MyKafkaConsumer() {
        super();
    }
    // 实例
    private static final MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer();

    public synchronized static MyKafkaConsumer getMyKafkaConsumerInstance(Properties properties) {
        if (null == consumer) {
            consumer = new KafkaConsumer<>(properties);
        }
        return myKafkaConsumer;
    }

    public synchronized static MyKafkaConsumer getMyKafkaConsumerInstance(String bootstrapServers, String groupId) {
        if (null == consumer) {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);
            properties.put("group.id", groupId);
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 6291456);
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(properties);
        }
        return myKafkaConsumer;
    }

    public synchronized static MyKafkaConsumer getDefaultMyKafkaConsumerInstance() {
        if (null == consumer) {
            InputStream is = MyKafkaProducer.class.getClassLoader().getResourceAsStream("kafka.properties");
            Properties prop = new Properties();
            try {
                prop.load(is);
            } catch (IOException e) {
                logger.error("initKafkaConsumer cause error", e);
            } finally {
                if (null != is) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        logger.error("close kafka.properties fail", e);
                    }
                }
            }
            Properties properties = new Properties();

            properties.put("bootstrap.servers", prop.getProperty("bootstrap.servers"));
            properties.put("group.id", prop.getProperty("group.id"));
            properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 6291456);
            properties.put("enable.auto.commit", null == prop.getProperty("enable.auto.commit") ? "true" : prop.getProperty("enable.auto.commit"));
            properties.put("auto.commit.interval.ms", null == prop.getProperty("auto.commit.interval.ms") ? "1000" : prop.getProperty("auto.commit.interval.ms"));
            properties.put("key.deserializer", null == prop.getProperty("key.deserializer") ? "org.apache.kafka.common.serialization.StringDeserializer" : prop.getProperty("key.deserializer"));
            properties.put("value.deserializer", null == prop.getProperty("value.deserializer") ? "org.apache.kafka.common.serialization.StringDeserializer" : prop.getProperty("value.deserializer"));
            consumer = new KafkaConsumer<>(properties);
        }
        return myKafkaConsumer;
    }
    public KafkaConsumer<String, String> getConsumer(){
        return consumer;
    }
}
