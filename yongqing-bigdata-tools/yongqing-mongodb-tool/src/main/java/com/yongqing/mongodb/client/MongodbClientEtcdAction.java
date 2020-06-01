package com.yongqing.mongodb.client;

import com.yongqing.etcd.action.Action;

import java.util.Properties;

/**
 *
 */
public class MongodbClientEtcdAction implements Action {
    @Override
    public void doAction(Properties properties, Properties properties1) {
        if (null != properties.getProperty("mongodbUrl") && null != properties1.getProperty("mongodbUrl") && !properties.getProperty("mongodbUrl").equals(properties1.getProperty("mongodbUrl"))) {
            if (null != MongodbClient.mongoClient) {
                MongodbClient.getInstanceByEtcd().close();
            }
            if (null != MongodbPoolClient.mongoClientList) {
                MongodbPoolClient.getInstanceByEtcd().close();
            }
        }
    }
}
