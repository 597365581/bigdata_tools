package com.yongqing.elasticsearch.client;

/**
 *
 */
public class DefaultSingletonElasticSearchPoolClient {

    private static volatile ElasticSearchPoolClient elasticSearchPoolClient = null;

    private DefaultSingletonElasticSearchPoolClient(){
        super();
    }

    public synchronized static ElasticSearchPoolClient getInstance(String[] hostsAndPorts, Integer poolSize) {
        if (null == elasticSearchPoolClient) {
            elasticSearchPoolClient = new ElasticSearchPoolClient(hostsAndPorts,poolSize);
        }
        return elasticSearchPoolClient;
    }

    public synchronized static void close(){
        if (null != elasticSearchPoolClient) {
            elasticSearchPoolClient.close();
            elasticSearchPoolClient = null;
        }
    }
}
