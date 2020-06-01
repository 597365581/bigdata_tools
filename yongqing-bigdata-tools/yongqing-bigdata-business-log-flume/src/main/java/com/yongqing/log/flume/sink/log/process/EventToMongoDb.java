package com.yongqing.log.flume.sink.log.process;


import com.mongodb.BasicDBObject;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.mongodb.client.MongodbClient;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class EventToMongoDb extends AbstractEventTo {
    private static final Logger logger = LoggerFactory
            .getLogger(EventToMongoDb.class);

    public EventToMongoDb() {
        logList = new ArrayList<Object>();
    }

    public EventToMongoDb(List<Object> logList) {
        this.logList = logList;
    }

    public void execute(String logType) {
        super.execute(logType, (String docId, Map<String, Object> customizeField, String dataBaseName, String tableName) -> {
            try {
                //1 替换模式  2 更新模式
                if (null != EtcdUtil.getLocalPropertie("insertMode") && EtcdUtil.getLocalPropertie("insertMode").equals("1")) {
                    //替换模式
                    Document documentQuery = new Document("_id", docId);
                    Document document = new Document("_id", docId);
                    document.putAll(customizeField);
                    ReplaceOptions replaceOptions = new ReplaceOptions();
                    replaceOptions.upsert(true);
                    MongodbClient.getInstance(EtcdUtil.getLocalPropertie("mongodbUrl")).replaceDoc(dataBaseName, tableName, documentQuery, document, replaceOptions);
                } else if ((null == EtcdUtil.getLocalPropertie("insertMode")) || ((null != EtcdUtil.getLocalPropertie("insertMode") && EtcdUtil.getLocalPropertie("insertMode").equals("2")))) {
                    //更新模式
                    BasicDBObject searchDoc = new BasicDBObject();
                    Document document = new Document();
                    searchDoc.append("_id", docId);
                    document.put("$set", customizeField);
                    UpdateOptions updateOptions = new UpdateOptions();
                    updateOptions.upsert(true);
                    MongodbClient.getInstance(EtcdUtil.getLocalPropertie("mongodbUrl")).updateDoc(dataBaseName, tableName, searchDoc, document, updateOptions);
                } else {
                    throw new RuntimeException("insertMode is not support,please check...");
                }
                logger.info("start to insert or update to Mongodb...docId:{}", docId);

            } catch (Throwable e) {
                logger.info("MongodbClient replaceDoc cause Exception", e);
            }
        });
    }
}
