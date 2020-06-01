package com.yongqing.log.flume.sink.log.process;

import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hdfs.tool.DefaultHdfsClient;
import com.yongqing.hive.tool.client.DefaultHiveMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.yongqing.hdfs.tool.constant.Constants.FS_DEFAULTFS;

/**
 *
 */
public class EventToHiveByNoTransaction extends AbstractEventTo implements Action {
    private static final Logger logger = LoggerFactory
            .getLogger(EventToHiveByNoTransaction.class);

    public EventToHiveByNoTransaction() {
        logList = new ArrayList<Object>();
    }

    public EventToHiveByNoTransaction(List<Object> logList) {
        this.logList = logList;
    }

    @Override
    public void execute(String logType) {
        super.execute(logType, (String docId, Map<String, Object> customizeField, String dataBaseName, String tableName) -> {
            StringBuilder stringBuilder = new StringBuilder();
            try {
                Map<String, String> tableFormat = DefaultHiveMetaStoreClient.getHiveMetaStoreClient(EtcdUtil.getLocalPropertie("hiveMetaStoreUri")).getTableSerialization(dataBaseName, tableName);
                String rowDelimited = tableFormat.get("field.delim");
                String lineDelimited = tableFormat.get("line.delim");
                //String serializationFormat = tableFormat.get("serialization.format");
                if (null == rowDelimited || null == lineDelimited) {
                    throw new RuntimeException("rowDelimited or lineDelimited is null...");
                }
                List<String> fields = DefaultHiveMetaStoreClient.getHiveMetaStoreClient(EtcdUtil.getLocalPropertie("hiveMetaStoreUri")).getFields(dataBaseName, tableName);
                logger.info("hive fields:{}", GsonUtil.gson.toJson(fields));
                customizeField = convertMapKeyToLowerCase(customizeField);
                if (null != fields && fields.size() > 0) {
                    int i = 1;
                    for (String field : fields) {
                        if (customizeField.containsKey(field)) {
                            stringBuilder.append(customizeField.get(field) instanceof String ? (String) customizeField.get(field) : GsonUtil.gson.toJson(customizeField.get(field)));
                        } else {
                            stringBuilder.append("");
                        }
                        if (fields.size() != i) {
                            stringBuilder.append(rowDelimited);
                        }
                        i = i + 1;
                    }
                    stringBuilder.append(lineDelimited);
                    String fileHdfspath = DefaultHiveMetaStoreClient.getHiveMetaStoreClient(EtcdUtil.getLocalPropertie("hiveMetaStoreUri")).getTableLocation(dataBaseName, tableName);
                    logger.info("hdfs file address:{}", fileHdfspath);
                    String path = fileHdfspath.replace(DefaultHdfsClient.getHdfsClient().getConfiguration().get(FS_DEFAULTFS), "");
                    logger.info("hdfs file path:{}", path);
                    List<String> files = DefaultHdfsClient.getHdfsClient().listFiles(path, false);
                    String file = EtcdUtil.getLocalPropertie(dataBaseName + "_" + tableName + "_hdfsFile");
                    if (null == file && null != files) {
                        file = files.get(0);
                    }
                    logger.info("hdfs file name :{}", file);
                    // TODO 分布式锁处理，append 不支持多线程处理。
                    DefaultHdfsClient.getHdfsClient().appendToFileByWriteBytes(path, file, 1000, stringBuilder.toString());
                    logger.info("write data:{} to hdfs success", stringBuilder.toString());
                } else {
                    logger.error("dataBaseName:{},tableName:{} has no fields", dataBaseName, tableName);
                }
            } catch (Throwable e) {
                logger.error("execute cause Exception", e);
            }
        });
    }

    @Override
    public void doAction(Properties oldProp, Properties newProp) {
        if (null != oldProp.getProperty("hiveMetaStoreUri") && null != newProp.getProperty("hiveMetaStoreUri") && !oldProp.getProperty("hiveMetaStoreUri").equals(newProp.getProperty("hiveMetaStoreUri"))) {
            DefaultHiveMetaStoreClient.close();
            try {
                DefaultHdfsClient.close();
            } catch (Exception e) {
                logger.error("hdfs client close cause error", e);
            }

        }
    }

    private Map<String, Object> convertMapKeyToLowerCase(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        map.forEach((k, v) -> {
            result.put(k.toLowerCase(), v);
        });
        return result;
    }
}
