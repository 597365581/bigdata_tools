package com.yongqing.hive.tool.client;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.yongqing.hive.tool.constant.Constants.HIVEMETASTOREURIS;

/**
 *
 */
@Log4j2
public class HiveMetaStoreClient implements Client {

    private volatile IMetaStoreClient metaStoreClient;
    private HiveConf conf;
    private Properties properties;
    private String hiveMetastoreUris;

    public HiveMetaStoreClient(HiveConf conf) {
        super();
        this.conf = conf;
        init(conf);
    }

    public HiveMetaStoreClient(Properties properties) {
        super();
        this.properties = properties;
        init(properties);
    }

    public HiveMetaStoreClient(String hiveMetastoreUris) {
        super();
        this.hiveMetastoreUris = hiveMetastoreUris;
        init(hiveMetastoreUris);
    }

    public IMetaStoreClient getMetaStoreClient() {
        return this.metaStoreClient;
    }

    public List<String> getFields(String db, String table) throws TException {
        List<String> list = new ArrayList<>();
        getMetaStoreClient().getSchema(db, table).forEach(fieldSchema -> {
            list.add(fieldSchema.getName());
        });
        return list;
    }

    public Table getTable(String db, String table) throws TException {
        return getMetaStoreClient().getTable(db, table);
    }

    public Database getDatabase(String db) throws TException {
        return getMetaStoreClient().getDatabase(db);
    }

    public String getTableLocation(String db, String table) throws TException {
        return getTable(db, table).getSd().getLocation();
    }

    public String getDatabaseLocationUri(String db) throws TException {
        return getDatabase(db).getLocationUri();
    }

    public Map<String, String> getTableParameters(String db, String table) throws TException {
        return getTable(db, table).getParameters();
    }

    public int getTableNumBuckets(String db, String table) throws TException {
        return getTable(db, table).getSd().getNumBuckets();
    }

    public Map<String,String> getTableSerialization(String db, String table) throws TException {
       return getTable(db, table).getSd().getSerdeInfo().getParameters();
    }

    private synchronized void init(HiveConf conf) {
        try {
            if (null == metaStoreClient) {
                metaStoreClient = new org.apache.hadoop.hive.metastore.HiveMetaStoreClient(conf);
            }
        } catch (MetaException e) {
            log.error("getMetaStoreClient cause Exception", e);
        }
    }

    private synchronized void init(Properties conf) {
        if (null == metaStoreClient) {
            HiveConf hiveConf = new HiveConf(getClass());
            hiveConf.set(HIVEMETASTOREURIS, conf.getProperty(HIVEMETASTOREURIS));
            init(hiveConf);
        }
    }

    private synchronized void init(String hiveMetastoreUris) {
        Properties conf = new Properties();
        conf.put(HIVEMETASTOREURIS, hiveMetastoreUris);
        init(conf);
    }


    @Override
    public void close() {
        if (null != metaStoreClient) {
            metaStoreClient.close();
        }
    }

    public void reconnect() throws MetaException {
        metaStoreClient.reconnect();
    }


}
