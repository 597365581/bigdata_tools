package com.yongqing.zookeeper.tool.client;
import org.apache.curator.RetryPolicy;


/**
 *
 */
public class DefaultZKClient {

    public volatile static AbstractZKClient abstractZKClient;

    public synchronized static AbstractZKClient getAbstractZKClient(String zookeeperConnectionString, RetryPolicy retryPolicy) {
        if (null == abstractZKClient) {
            abstractZKClient = new AbstractZKClient(zookeeperConnectionString, retryPolicy);
        }
        return abstractZKClient;
    }

    public synchronized static AbstractZKClient getAbstractZKClient(String zookeeperConnectionString, Integer zookeeperRetrySleep, Integer zookeeperRetryMaxtime) {
        if (null == abstractZKClient) {
            abstractZKClient = new AbstractZKClient(zookeeperConnectionString, zookeeperRetrySleep, zookeeperRetryMaxtime);
        }
        return abstractZKClient;
    }

    public synchronized static AbstractZKClient getAbstractZKClient(String zookeeperConnectionString, Integer zookeeperRetrySleep, Integer zookeeperRetryMaxtime, Integer zookeeperSessionTimeout, Integer zookeeperConnectionTimeout) {
        if (null == abstractZKClient) {
            abstractZKClient = new AbstractZKClient(zookeeperConnectionString, zookeeperRetrySleep, zookeeperRetryMaxtime, zookeeperSessionTimeout, zookeeperConnectionTimeout);
        }
        return abstractZKClient;
    }
}

