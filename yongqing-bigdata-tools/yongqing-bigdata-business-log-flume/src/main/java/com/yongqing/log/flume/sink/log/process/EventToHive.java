package com.yongqing.log.flume.sink.log.process;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.reflect.TypeToken;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hive.tool.HiveJsonDataSerializerImpl;
import com.yongqing.hive.tool.HiveWriter;
import com.yongqing.log.flume.sink.bean.DataBaseConfig;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.yongqing.common.bigdata.tool.GsonUtil.gson;

/**
 * Event 同步到hive中
 */
public class EventToHive extends AbstractEventTo implements Action {

    private static final Logger logger = LoggerFactory
            .getLogger(EventToHive.class);
    public static List<HiveWriter> hiveWriterList;

    public EventToHive() {
        logList = new ArrayList<Object>();
    }

    public EventToHive(List<Object> logList) {
        this.logList = logList;
    }

    @Override
    public void execute(String logType) {
        super.execute(logType, (String docId, Map<String, Object> customizeField, String dataBaseName, String tableName) -> {
            if (null != customizeField) {
                try {
                    if (null != hiveWriterList && hiveWriterList.size() > 0) {
                        for (HiveWriter hiveWriter : hiveWriterList) {
                            if (hiveWriter.getEndPointDatabase().equals(dataBaseName) && hiveWriter.getEndPointTable().equals(tableName)) {
                                logger.info("start to write event:{}", GsonUtil.gson.toJson(customizeField));
                                hiveWriter.write((GsonUtil.gson.toJson(customizeField).getBytes()));
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("execute cause Exception", e);
                }
            }
        });
    }

    private void resetHiveWriter() {
        synchronized (EventToHive.class) {
            if (null != hiveWriterList && hiveWriterList.size() > 0) {
                for (HiveWriter hiveWriter : hiveWriterList) {
                    try {
                        hiveWriter.close();
                        hiveWriter.closeCallTimeoutPool();
                    } catch (InterruptedException e) {
                        logger.error("hiveWriter.close() cause Exception", e);
                    }
                }
                hiveWriterList.clear();
                ExecutorService callTimeoutPool = Executors.newFixedThreadPool(1,
                        new ThreadFactoryBuilder().setNameFormat("hive sink").build());
                List<DataBaseConfig> dataBaseConfigList = gson.fromJson(EtcdUtil.getLocalPropertie("dataBaseConfig"), new TypeToken<List<DataBaseConfig>>() {
                }.getType());
                for (DataBaseConfig dataBaseConfig : dataBaseConfigList) {
                    HiveWriter hiveWriter = null;
                    try {
                        hiveWriter = new HiveWriter(new HiveEndPoint(EtcdUtil.getLocalPropertie("hiveMetaStoreUri"), dataBaseConfig.getDatabaseName(), dataBaseConfig.getTableName(), null), 100, false, 10000, callTimeoutPool, EtcdUtil.getLocalPropertie("hiveUser"), new HiveJsonDataSerializerImpl());
                    } catch (HiveWriter.ConnectException e) {
                        logger.error("resetHiveWriter create cause Exception", e);
                    } catch (InterruptedException e) {
                        logger.error("resetHiveWriter create cause Exception", e);
                    }
                    hiveWriterList.add(hiveWriter);
                }
            }
        }
    }

    @Override
    public void doAction(Properties oldProp, Properties newProp) {
        if (null != oldProp.getProperty("hiveMetaStoreUri") && null != newProp.getProperty("hiveMetaStoreUri") && !oldProp.getProperty("hiveMetaStoreUri").equals(newProp.getProperty("hiveMetaStoreUri"))) {
            resetHiveWriter();
            return;
        }
        if (null != oldProp.getProperty("dataBaseConfig") && null != newProp.getProperty("dataBaseConfig") && !oldProp.getProperty("dataBaseConfig").equals(newProp.getProperty("dataBaseConfig"))) {
            resetHiveWriter();
        }
    }
}
