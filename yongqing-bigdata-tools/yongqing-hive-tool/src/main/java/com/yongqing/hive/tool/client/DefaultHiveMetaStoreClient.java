package com.yongqing.hive.tool.client;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

/**
 *
 */
public class DefaultHiveMetaStoreClient {

    private static volatile HiveMetaStoreClient hiveMetaStoreClient;

    public static synchronized HiveMetaStoreClient getHiveMetaStoreClient(HiveConf conf) {
        if (null == hiveMetaStoreClient) {
            hiveMetaStoreClient = new HiveMetaStoreClient(conf);
            return hiveMetaStoreClient;
        }
        return hiveMetaStoreClient;
    }

    public static synchronized HiveMetaStoreClient getHiveMetaStoreClient(Properties conf) {
        if (null == hiveMetaStoreClient) {
            hiveMetaStoreClient = new HiveMetaStoreClient(conf);
            return hiveMetaStoreClient;
        }
        return hiveMetaStoreClient;
    }

    public static synchronized HiveMetaStoreClient getHiveMetaStoreClient(String hiveMetastoreUris) {
        if (null == hiveMetaStoreClient) {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveMetastoreUris);
            return hiveMetaStoreClient;
        }
        return hiveMetaStoreClient;
    }

    public static synchronized void close() {
        if (null != hiveMetaStoreClient) {
            hiveMetaStoreClient.close();
            hiveMetaStoreClient = null;
        }
    }

    public static void main(String[] args) throws Exception {
//        DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getFields("customer","test_flume2").forEach(s->{
//            System.out.println("==============="+s);
//        });
//        System.out.println(DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getTable("customer"," t_cus_info_mail_list").getSd().getLocation());
//        DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getTable("customer"," t_cus_info_mail_list").getSd().getParameters().forEach((k,v)->{
//          System.out.println("========k:"+k+"  v:"+v);
//        });
//        DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getTable("customer"," t_cus_info_mail_list").getParameters().forEach((k,v)->{
//            System.out.println("========k:"+k+"  v:"+v);
//        });
//        DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getTable("customer"," t_cus_info_mail_list");
//        System.out.println(  DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getTableSerialization("customer"," t_cus_info_mail_list"));
//
//        System.out.println(DefaultHiveMetaStoreClient.getHiveMetaStoreClient("thrift://121.40.132.57:9083").getMetaStoreClient().getDatabase("customer").getLocationUri());
    }

}
