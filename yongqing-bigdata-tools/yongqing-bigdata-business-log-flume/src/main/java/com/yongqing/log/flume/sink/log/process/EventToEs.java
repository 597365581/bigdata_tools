package com.yongqing.log.flume.sink.log.process;


import com.yongqing.elasticsearch.client.DefaultSingletonElasticSearchClient;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;



/**
 *
 */
public class EventToEs extends AbstractEventTo implements Action {
    private static final Logger logger = LoggerFactory
            .getLogger(EventToEs.class);

    public EventToEs() {
        super();
        logList = new ArrayList<Object>();
        DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(","));
    }
    public EventToEs(List<Object> logList) {
        this.logList = logList;
        DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(","));
    }
    public void execute(String logType){
        super.execute(logType,(String docId, Map<String, Object> customizeField, String dataBaseName, String tableName)->{
            try {
                DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(",")).upDdateocAsUpsert(dataBaseName, tableName, docId, customizeField);
            } catch (Throwable e) {
                logger.info("elasticSearchClient upDdateocAsUpsert cause Exception", e);
            }
        } );
    }
    @Override
    public void doAction(Properties properties, Properties properties1) {
        if (null != properties.getProperty("esHosts") && null != properties1.getProperty("esHosts") && !properties.getProperty("esHosts").equals(properties1.getProperty("esHosts"))) {
            synchronized (this) {
                try {
                    DefaultSingletonElasticSearchClient.close();
                } catch (Exception e) {
                    logger.error("es connection close cause Exception", e);
                }
                DefaultSingletonElasticSearchClient.getInstance(EtcdUtil.getLocalPropertie("esHosts").split(","));
            }
        }
    }
}
