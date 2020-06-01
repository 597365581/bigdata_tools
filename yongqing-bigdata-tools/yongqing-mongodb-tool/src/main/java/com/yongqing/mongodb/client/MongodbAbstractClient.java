package com.yongqing.mongodb.client;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 */
public abstract class MongodbAbstractClient extends MongodbAbstractBaseClient {
    //mongoClient连接
    protected static volatile MongoClient mongoClient;

    @Override
    public synchronized void close() {
        if (null != mongoClient) {
            mongoClient.close();
            mongoClient = null;
        }
    }


    public MongoDatabase getDatabase(String dataBaseName) {
        return mongoClient.getDatabase(dataBaseName);
    }

    public List<String> listCollectionNames(String dataBaseName) {
        List<String> stringList = new ArrayList<String>();
        mongoClient.getDatabase(dataBaseName).listCollectionNames().forEach((Consumer<? super String>) t -> {
            stringList.add(t);
        });
        return stringList;
    }
}
