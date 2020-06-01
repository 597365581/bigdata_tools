package com.yongqing.hbase.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * hbase 客户端操作
 */
public class HbaseClient extends HbaseAbstractClient {
    //log
    private static final Logger logger = LoggerFactory.getLogger(HbaseClient.class);
    // 实例
    private static final HbaseClient hbaseClient = new HbaseClient();
    //hadoop的配置 hbase 初始化链接的时候需要hadoop的配置
    protected static org.apache.hadoop.conf.Configuration conf;
    // 加锁变量，避免锁整个类
    protected static Object object = new Object();
    private HbaseClient() {
        super();
    }
    //单例模式，确保链接只初始化一次
    public synchronized static HbaseClient getInstance() {
            if (null == connection || connection.isClosed()) {
                hbaseClient.initHbaseConnection();
                if(null != connection){
                    logger.info("connect online hbase success!");
                }
                else{
                    logger.info("connect online hbase failed!");
                }
            }
        return hbaseClient;
    }
    //单例模式，确保链接只初始化一次
    public synchronized static HbaseClient getInstance(String zkClient,String zkClientPort) {
        if (null == connection || connection.isClosed()) {
            hbaseClient.initHbaseConnection(zkClient,zkClientPort);
            if(null != connection){
                logger.info("use zkClient:{},zkClientPort:{} connect online hbase success!",zkClient,zkClientPort);
            }
            else{
                logger.info("use zkClient:{},zkClientPort:{} connect online hbase failed!",zkClient,zkClientPort);
            }
        }
        return hbaseClient;
    }
    //单例模式，确保链接只初始化一次
    public synchronized static HbaseClient getInstance(org.apache.hadoop.conf.Configuration conf,User user) {
        if (null == connection || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(conf, user);
            } catch (IOException e) {
                logger.error("initHbaseConnection cause error",e);
            }
            if(null != connection){
                logger.info("connect online hbase success!");
            }
            else{
                logger.info("connect online hbase failed!");
            }
        }
        return hbaseClient;
    }
    public  void initHbaseConnection(){
        initHbaseConnection(null,null);
    }
    /**
     * 初始化hbase的链接
     */
    private void initHbaseConnection(String zkClient,String zkClientPort) {
        // 获取资源文件
        InputStream is = HbaseClient.class.getClassLoader().getResourceAsStream("hbase.properties");
        //属性列表
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            logger.error("read hbase.properties fail", e);
        } finally {
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("close hbase.properties fail", e);
                }
            }
        }
        //创建hbase配置
        conf = HBaseConfiguration.create();
        System.getProperties().setProperty("HADOOP_USER_NAME",prop.getProperty("hadoopUser"));
        System.getProperties().setProperty("HADOOP_GROUP_NAME",prop.getProperty("hadoopUserGroup"));
//        System.getProperties().setProperty("user.home",prop.getProperty("userHome"));
//        System.getProperties().setProperty("user.dir",prop.getProperty("userDir"));
        //zk服务器地址
        conf.set(HConstants.ZOOKEEPER_QUORUM,null==zkClient?prop.getProperty("zkClient"):zkClient);
        //zk链接端口
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, null==zkClientPort?prop.getProperty("zkClientPort"):zkClientPort);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, prop.getProperty("zkZnodeparent"));
        User user = User.create(UserGroupInformation.createRemoteUser(prop.getProperty("hadoopUser")));
        //重试次数
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, prop.getProperty("hbaseClientRetriesNumber"));
        // 通常的客户端暂停时间。最多的用法是客户端在重试前的等待时间
        conf.set(HConstants.HBASE_CLIENT_PAUSE,prop.getProperty("hbaseClientPause"));
        conf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, prop.getProperty("hbaseRpcTimeOut"));
        conf.set("ipc.socket.timeout", prop.getProperty("ipcScoketTimeout"));
        conf.set("zookeeper.recovery.retry.intervalmill", prop.getProperty("zkRecoveryRetryIntervalmill"));
        //scan超时
        conf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, prop.getProperty("hbaseClientScannerTimeOut"));
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,prop.getProperty("hbaseClientOperationTimeout"));
        try {
            connection = ConnectionFactory.createConnection(conf, user);
        } catch (IOException e) {
            logger.error("initHbaseConnection cause error",e);
        }
    }
}
