package com.yongqing.rocketmq.elasticsearch.client;
import com.google.gson.Gson;
import com.yongqing.rocketmq.elasticsearch.ElasticSearchEventSerializer;
import com.yongqing.rocketmq.elasticsearch.IndexNameBuilder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ElasticSearchClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
    private static final String INDEX_OPERATION_NAME = "index";
    private static final String INDEX_PARAM = "_index";
    private static final String TYPE_PARAM = "_type";
    private static final String TTL_PARAM = "_ttl";
    private static final String BULK_ENDPOINT = "_bulk";
    private final ElasticSearchEventSerializer serializer;
    private final RoundRobinList<String> serversList;
    private StringBuilder bulkBuilder;

    public ElasticSearchClient getClient(String[] hostNames, ElasticSearchEventSerializer serializer){
      return   new ElasticSearchClient(hostNames,serializer);
    }

    public ElasticSearchClient(String[] hostNames,
                               ElasticSearchEventSerializer serializer) {
        for (int i = 0; i < hostNames.length; ++i) {
            if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
                hostNames[i] = "http://" + hostNames[i];
            }
        }
        this.serializer = serializer;
        serversList = new RoundRobinList<String>(Arrays.asList(hostNames));
        bulkBuilder = new StringBuilder();
    }

    @Override
    public void close() {

    }

    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType, long ttlMs) throws Exception {
        BytesReference content = serializer.getContentBuilder(event).bytes();
        Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
        Map<String, String> indexParameters = new HashMap<String, String>();
        indexParameters.put(INDEX_PARAM, indexNameBuilder.getIndexName(event));
        indexParameters.put(TYPE_PARAM, indexType);
//        if (ttlMs > 0) {
//            indexParameters.put(TTL_PARAM, Long.toString(ttlMs));
//        }
        parameters.put(INDEX_OPERATION_NAME, indexParameters);
        Gson gson = new Gson();
        synchronized (bulkBuilder) {
            bulkBuilder.append(gson.toJson(parameters));
            bulkBuilder.append("\n");
            bulkBuilder.append(content.utf8ToString());
            bulkBuilder.append("\n");
        }
    }

    @Override
    public void execute() throws Exception {
        int statusCode = 0, triesCount = 0;
        HttpResponse response = null;
        String entity;
        synchronized (bulkBuilder) {
            entity = bulkBuilder.toString();
            bulkBuilder = new StringBuilder();
        }
        while (statusCode != HttpStatus.SC_OK && triesCount < serversList.size()) {
            triesCount++;
            String host = serversList.get();
            String url = host + "/" + BULK_ENDPOINT;
            HttpPost httpRequest = new HttpPost(url);
            httpRequest.setHeader("Content-Type","application/json");
            httpRequest.setEntity(new StringEntity(entity));
            response = ElasticSearchHttpClient.getHttpClient().execute(httpRequest);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity responseEntity=response.getEntity();
            logger.info("Status code from elasticsearch: " + statusCode);
            if ( null != responseEntity) {
                logger.info("Status message from elasticsearch: " +
                        EntityUtils.toString(responseEntity, "UTF-8"));
            }
        }
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED && statusCode != HttpStatus.SC_ACCEPTED) {
            if (null != response && null != response.getEntity()) {
                throw new EventDeliveryException(EntityUtils.toString(response.getEntity(), "UTF-8"));
            } else {
                throw new EventDeliveryException("Elasticsearch status code was: " + statusCode);
            }
        }
    }
    @Override
    public void configure(Context context) {

    }
}
