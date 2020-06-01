package com.yongqing.mongodb.client;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 *
 */
@Log4j2
public abstract class MongodbAbstractPoolClient extends MongodbAbstractBaseClient{
    //mongoClient连接池
    protected static volatile CopyOnWriteArrayList<MongoClient> mongoClientList;

    protected static Integer poolSize=3;
    @Override
    public synchronized void close() {
        if (null != mongoClientList && mongoClientList.size()>0) {
            for(MongoClient mongoClient:mongoClientList){
                try{
                    if(null!=mongoClient){
                        mongoClient.close();
                        mongoClient=null;
                    }
                }
                catch (Throwable e){
                    log.error("close mongoClient cause Exception",e);
                }
            }
            mongoClientList=null;
        }
    }
    public MongoClient getMongoClient(){
        if(null != mongoClientList && mongoClientList.size()>0){
            return mongoClientList.get(new Random().nextInt(mongoClientList.size()));
        }
        else {
            throw new RuntimeException("mongoClient pool is null or pool size is 0,please check.....");
        }
    }
    public MongoDatabase getDatabase(String dataBaseName) {
        return getMongoClient().getDatabase(dataBaseName);
    }
    public List<String> listCollectionNames(String dataBaseName) {
        List<String> stringList = new ArrayList<String>();
        getMongoClient().getDatabase(dataBaseName).listCollectionNames().forEach((Consumer<? super String>) t -> {
            stringList.add(t);
        });
        return stringList;
    }
}
