package com.yongqing.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * es操作
 * https://www.cnblogs.com/reycg-blog/p/9931482.html
 */
public class ElasticSearchClient extends ElasticSearchAbstractClient {
    private String[] hostsAndPorts;
    private volatile RestHighLevelClient client;
    private volatile RestClientBuilder restClientBuilder;

    public ElasticSearchClient(String[] hostsAndPorts) {
        this.hostsAndPorts = hostsAndPorts;
    }

    public RestHighLevelClient getClient() {
        // RestHighLevelClient client = null;
        synchronized (this) {
            if (null == client) {
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
                client = new RestHighLevelClient(restClientBuilder);
            }
        }
        return client;
    }

    @Override
    public synchronized void close() {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            client = null;
            restClientBuilder = null;
        }
    }
}