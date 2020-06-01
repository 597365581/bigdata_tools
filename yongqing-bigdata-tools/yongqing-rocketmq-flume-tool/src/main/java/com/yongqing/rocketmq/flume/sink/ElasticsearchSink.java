package com.yongqing.rocketmq.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.yongqing.rocketmq.elasticsearch.ElasticSearchEventSerializer;
import com.yongqing.rocketmq.elasticsearch.ElasticSearchSinkConstants;
import com.yongqing.rocketmq.elasticsearch.IndexNameBuilder;
import com.yongqing.rocketmq.elasticsearch.client.ElasticSearchClient;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * elasticsearch  Sink
 */
public class ElasticsearchSink extends AbstractSink implements Configurable, BatchSizeSupported {
    private final CounterGroup counterGroup = new CounterGroup();
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticsearchSink.class);
    private String[] serverAddresses = null;
    private static final int defaultBatchSize = 100;
    private String indexName = ElasticSearchSinkConstants.DEFAULT_INDEX_NAME;
    private String indexType = ElasticSearchSinkConstants.DEFAULT_INDEX_TYPE;
    private int batchSize = defaultBatchSize;
    private long ttlMs = ElasticSearchSinkConstants.DEFAULT_TTL;
    private final Pattern pattern = Pattern.compile(ElasticSearchSinkConstants.TTL_REGEX,
            Pattern.CASE_INSENSITIVE);
    private Matcher matcher = pattern.matcher("");
    private String clusterName = ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;
    private ElasticSearchClient client;
    ElasticSearchEventSerializer elasticSearchEventSerializer;
    private Context elasticSearchClientContext = null;
    private SinkCounter sinkCounter;
    private IndexNameBuilder indexNameBuilder;

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("processing...");
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        try {
            txn.begin();
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();

                if (event == null) {
                    break;
                }
                String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());
                client.addEvent(event, indexNameBuilder, realIndexType, ttlMs);
            }

            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }

                sinkCounter.addToEventDrainAttemptCount(count);
                client.execute();
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable ex) {
            try {
                logger.error("ElasticsearchSink sink cause exception",ex);
                txn.rollback();
                counterGroup.incrementAndGet("transaction.rollback");
            } catch (Exception ex2) {
                ex2.printStackTrace();
                logger.error(
                        "Exception in rollback. Rollback might not have been successful.",
                        ex2);
            }

            if (ex instanceof Error || ex instanceof RuntimeException) {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                Throwables.propagate(ex);
            } else {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                throw new EventDeliveryException(
                        "Failed to commit transaction. Transaction rolled back.", ex);
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void start() {
        logger.info("ElasticSearch sink {} started");
        sinkCounter.start();
        client = new ElasticSearchClient(serverAddresses, elasticSearchEventSerializer);
        client.configure(elasticSearchClientContext);
        sinkCounter.incrementConnectionCreatedCount();
        super.start();
    }

    @Override
    public void stop() {
        logger.info("ElasticSearch sink {} stopping");
        if (null != client) {
            client.close();
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public long getBatchSize() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.HOSTNAMES))) {
            serverAddresses = StringUtils.deleteWhitespace(
                    context.getString(ElasticSearchSinkConstants.HOSTNAMES)).split(",");
        }
        Preconditions.checkState(serverAddresses != null
                && serverAddresses.length > 0, "Missing Param:" + ElasticSearchSinkConstants.HOSTNAMES);
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_NAME))) {
            this.indexName = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
        }
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_TYPE))) {
            this.indexType = context.getString(ElasticSearchSinkConstants.INDEX_TYPE);
        }
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.BATCH_SIZE))) {
            this.batchSize = Integer.parseInt(context.getString(ElasticSearchSinkConstants.BATCH_SIZE));
        }
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.TTL))) {
            this.ttlMs = parseTTL(context.getString(ElasticSearchSinkConstants.TTL));
            Preconditions.checkState(ttlMs > 0, ElasticSearchSinkConstants.TTL
                    + " must be greater than 0 or not set.");
        }
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.CLUSTER_NAME))) {
            this.clusterName = context.getString(ElasticSearchSinkConstants.CLUSTER_NAME);
        }
        elasticSearchClientContext = new Context();
        elasticSearchClientContext.putAll(context.getSubProperties(ElasticSearchSinkConstants.CLIENT_PREFIX));
        String serializerClazz = ElasticSearchSinkConstants.DEFAULT_SERIALIZER_CLASS;
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.SERIALIZER))) {
            serializerClazz = context.getString(ElasticSearchSinkConstants.SERIALIZER);
        }
        Context serializerContext = new Context();
        serializerContext.putAll(context.getSubProperties(ElasticSearchSinkConstants.SERIALIZER_PREFIX));

        try{
            @SuppressWarnings("unchecked")
            Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
                    .forName(serializerClazz);
            Configurable serializer = clazz.newInstance();
            if(serializer instanceof ElasticSearchEventSerializer){
                elasticSearchEventSerializer = (ElasticSearchEventSerializer) serializer;
                elasticSearchEventSerializer.configure(serializerContext);
            }
            else {
                throw new IllegalArgumentException(serializerClazz
                        + " is not an ElasticSearchEventSerializer");
            }
        }
        catch (Exception e){
            logger.error("Could not instantiate event serializer.", e);
            Throwables.propagate(e);
        }
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        String indexNameBuilderClass = ElasticSearchSinkConstants.DEFAULT_INDEX_NAME_BUILDER_CLASS;
        if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConstants.INDEX_NAME_BUILDER))) {
            indexNameBuilderClass = context.getString(ElasticSearchSinkConstants.INDEX_NAME_BUILDER);
        }
        Context indexnameBuilderContext = new Context();
        serializerContext.putAll(
                context.getSubProperties(ElasticSearchSinkConstants.INDEX_NAME_BUILDER_PREFIX));

        try{
            @SuppressWarnings("unchecked")
            Class<? extends IndexNameBuilder> clazz
                    = (Class<? extends IndexNameBuilder>) Class
                    .forName(indexNameBuilderClass);
            indexNameBuilder = clazz.newInstance();
            indexnameBuilderContext.put(ElasticSearchSinkConstants.INDEX_NAME, indexName);
            indexNameBuilder.configure(indexnameBuilderContext);
        } catch (Exception e) {
            logger.error("Could not instantiate index name builder.", e);
            Throwables.propagate(e);
        }
        Preconditions.checkState(StringUtils.isNotBlank(indexName),
                "Missing Param:" + ElasticSearchSinkConstants.INDEX_NAME);
        Preconditions.checkState(StringUtils.isNotBlank(indexType),
                "Missing Param:" + ElasticSearchSinkConstants.INDEX_TYPE);
        Preconditions.checkState(StringUtils.isNotBlank(clusterName),
                "Missing Param:" + ElasticSearchSinkConstants.CLUSTER_NAME);
        Preconditions.checkState(batchSize >= 1, ElasticSearchSinkConstants.BATCH_SIZE
                + " must be greater than 0");
    }

    private long parseTTL(String ttl) {
        matcher = matcher.reset(ttl);
        while (matcher.find()) {
            if (matcher.group(2).equals("ms")) {
                return Long.parseLong(matcher.group(1));
            } else if (matcher.group(2).equals("s")) {
                return TimeUnit.SECONDS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("m")) {
                return TimeUnit.MINUTES.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("h")) {
                return TimeUnit.HOURS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("d")) {
                return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("w")) {
                return TimeUnit.DAYS.toMillis(7 * Integer.parseInt(matcher.group(1)));
            } else if (matcher.group(2).equals("")) {
                logger.info("TTL qualifier is empty. Defaulting to day qualifier.");
                return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
            } else {
                logger.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
                return 0;
            }
        }
        logger.info("TTL not provided. Skipping the TTL config by returning 0.");
        return 0;
    }
}
