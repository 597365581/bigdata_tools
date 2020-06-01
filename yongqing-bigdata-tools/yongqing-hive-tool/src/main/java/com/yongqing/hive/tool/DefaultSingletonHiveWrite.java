package com.yongqing.hive.tool;

import com.yongqing.hive.tool.pojo.ParameterPojo;
import com.yongqing.hive.tool.pojo.FieldDealPojo;

/**
 *
 */
public class DefaultSingletonHiveWrite {
    private static volatile HiveWrite hiveWrite = null;

    private DefaultSingletonHiveWrite() {
        super();
    }

    public synchronized static HiveWrite getDefaultSingletonHiveJsonWrite(String metaStoreUri, String proxyUser, String database, String table, String tzName) {
        if (null == hiveWrite) {
            hiveWrite = new HiveWrite(metaStoreUri, proxyUser, database, table, tzName,null,"JSON");
        }
        return hiveWrite;
    }

    public synchronized static HiveWrite getDefaultSingletonHiveWrite(String metaStoreUri, String proxyUser, String database, String table, String tzName,FieldDealPojo fieldDealPojo,String dataType) {
        if (null == hiveWrite) {
            hiveWrite = new HiveWrite(metaStoreUri, proxyUser, database, table, tzName,fieldDealPojo,dataType);
        }
        return hiveWrite;
    }

    public synchronized static HiveWrite getDefaultSingletonHiveWrite(String metaStoreUri, String proxyUser, String database, String table,FieldDealPojo fieldDealPojo,String dataType) {
        if (null == hiveWrite) {
            hiveWrite = new HiveWrite(metaStoreUri, proxyUser, database, table, null,fieldDealPojo,dataType);
        }
        return hiveWrite;
    }

    public synchronized static HiveWrite getDefaultSingletonHiveWrite(ParameterPojo parameterPojo, String dataType) {
        if (null == hiveWrite) {
            hiveWrite = new HiveWrite(parameterPojo.getMetaStoreUri(), parameterPojo.getProxyUser(), parameterPojo.getDatabase(), parameterPojo.getTable(), parameterPojo.getHivePartition(),parameterPojo.getHiveTxnsPerBatchAsk(),parameterPojo.getBatchSize(),parameterPojo.getIdleTimeout(),parameterPojo.getCallTimeout(),parameterPojo.getHeartBeatInterval(),parameterPojo.getMaxOpenConnections(),parameterPojo.getAutoCreatePartitions(),parameterPojo.getUseLocalTime(),parameterPojo.getTzName(),parameterPojo.getNeedRounding(),parameterPojo.getRoundUnit(),parameterPojo.getRoundValue(),parameterPojo.getFieldDealPojo(),dataType);
        }
        return hiveWrite;
    }
}
