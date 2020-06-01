package com.yongqing.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class ElasticSearchPoolClient extends ElasticSearchAbstractClient {

    private String[] hostsAndPorts;
    private volatile RestClientBuilder restClientBuilder;
    private volatile CopyOnWriteArrayList<RestHighLevelClient> poolList;
    private Integer poolSize;

    public ElasticSearchPoolClient(String[] hostsAndPorts, Integer poolSize) {
        this.hostsAndPorts = hostsAndPorts;
        this.poolSize = poolSize;
    }

    @Override
    public synchronized RestHighLevelClient getClient() {
        if (null == poolList) {
            List<HttpHost> httpHosts = new ArrayList<HttpHost>();
            if (hostsAndPorts.length > 0) {
                for (String hostsAndPort : hostsAndPorts) {
                    String[] hp = hostsAndPort.split(":");
                    httpHosts.add(new HttpHost(hp[0], Integer.valueOf(hp[1]), "http"));
                }
                if (null == restClientBuilder) {
                    restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
                }

            } else {
                if (null == restClientBuilder) {
                    restClientBuilder = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http"));
                }
            }
            for (int i = 0; i <= poolSize; i++) {
                poolList.add(new RestHighLevelClient(restClientBuilder));
            }
        }
        if (poolList.size() <= 0) {
            throw new RuntimeException("ElasticSearchPoolClient pool is null or pool size is 0,please check.....");
        }

        return poolList.get(new Random().nextInt(poolList.size()));
    }

    @Override
    public synchronized void close() {
        if (null != poolList && poolList.size() > 0) {
            for (RestHighLevelClient restHighLevelClient : poolList) {
                if (null != restHighLevelClient) {
                    try {
                        restHighLevelClient.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    restHighLevelClient = null;
                }
            }
            poolList = null;
            restClientBuilder= null;
        }
    }
}
