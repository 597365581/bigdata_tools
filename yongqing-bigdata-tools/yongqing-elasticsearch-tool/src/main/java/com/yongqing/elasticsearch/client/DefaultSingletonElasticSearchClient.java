package com.yongqing.elasticsearch.client;

/**
 *
 */
public class DefaultSingletonElasticSearchClient {

    private static volatile ElasticSearchClient elasticSearchClient = null;

    private DefaultSingletonElasticSearchClient() {
        super();
    }

    public synchronized static ElasticSearchClient getInstance(String[] hostsAndPorts) {
        if (null == elasticSearchClient) {
            elasticSearchClient = new ElasticSearchClient(hostsAndPorts);
        }
        return elasticSearchClient;
    }
    public synchronized static void close(){
        if (null != elasticSearchClient) {
            elasticSearchClient.close();
            elasticSearchClient = null;
        }
    }
}
