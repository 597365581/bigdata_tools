package com.yongqing.mongodb.client;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.yongqing.etcd.tools.EtcdUtil;
import lombok.extern.log4j.Log4j2;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *IMPORTANT（官网）http://mongodb.github.io/mongo-java-driver/3.11/driver/tutorials/connect-to-mongodb/
 Typically you only create one MongoClient instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

 All resource usage limits (e.g. max connections, etc.) apply per MongoClient instance.

 To dispose of an instance, call MongoClient.close() to clean up resources.
 */
@Log4j2
public class MongodbClient extends MongodbAbstractClient implements Client{
    //log
//    private static final Logger logger = LoggerFactory.getLogger(MongodbClient.class);

    // 实例
    private static final MongodbClient mongodbClient = new MongodbClient();
    private MongodbClient() {
        super();
    }
    public void intiClientConnection(){
        // 获取资源文件
        InputStream is = MongoClient.class.getClassLoader().getResourceAsStream("mongodb.properties");
        //属性列表
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            log.error("read mongodb.properties fail", e);
        } finally {
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    log.error("close mongodb.properties fail", e);
                }
            }
        }
         mongoClient = MongoClients.create(prop.getProperty("mongodbUrl"));
    }
    //单例模式获取

    /**
     * 官网说明
     * Typically you only create one MongoClient instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

     All resource usage limits (e.g. max connections, etc.) apply per MongoClient instance.

     To dispose of an instance, call MongoClient.close() to clean up resources.
     * @return
     */
    public synchronized static MongodbClient getInstance() {
        if (null == mongoClient) {
            mongodbClient.intiClientConnection();
            if(null != mongoClient){
                log.info("mongoClient init success!");
            }
            else{
                log.info("mongoClient init failed!");
            }
        }
        return mongodbClient;
    }
    public synchronized static MongodbClient getInstanceByEtcd() {
        if (null == mongoClient) {
            mongoClient = MongoClients.create(EtcdUtil.getLocalPropertie("mongodbUrl"));
            if(null != mongoClient){
                log.info("mongoClient init success!");
            }
            else{
                log.info("mongoClient init failed!");
            }
        }
        return mongodbClient;
    }
    public synchronized static MongodbClient getInstance(String mongodbUrl) {
        if (null == mongoClient) {
            mongoClient = MongoClients.create(mongodbUrl);
            if(null != mongoClient){
                log.info("mongoClient init success!");
            }
            else{
                log.info("mongoClient init failed!");
            }
        }
        return mongodbClient;
    }

}
