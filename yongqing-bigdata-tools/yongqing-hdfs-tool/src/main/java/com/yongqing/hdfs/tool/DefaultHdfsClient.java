package com.yongqing.hdfs.tool;


import com.yongqing.hdfs.tool.pojo.ParameterPojo;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.Properties;

import static com.yongqing.hdfs.tool.constant.Constants.*;

/**
 *
 */
@Log4j2
public class DefaultHdfsClient {

    private static volatile HdfsClient hdfsClient = null;

    private DefaultHdfsClient() {

    }

    public synchronized static HdfsClient getHdfsClient() {
        if (null == hdfsClient) {
            hdfsClient = new HdfsClient();
        }
        return hdfsClient;
    }

    public synchronized static HdfsClient getHdfsClient(Properties properties) {
        if (null == hdfsClient) {
            hdfsClient = new HdfsClient(properties);
        }
        return hdfsClient;
    }

    public synchronized static HdfsClient getHdfsClient(ParameterPojo parameterPojo) {
        Properties properties = new Properties();

        properties.put(JAVA_SECURITY_KRB5_CONF, parameterPojo.getJavaSecurityKrb5Conf());
        properties.put(JAVA_SECURITY_KRB5_CONF_PATH, parameterPojo.getJavaSecurityKrb5ConfPath());
        properties.put(HADOOP_SECURITY_AUTHENTICATION, parameterPojo.getHadoopSecurityAuthentication());
        properties.put(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, parameterPojo.getHadoopSecurityAuthenticationStartupState());
        properties.put(LOGIN_USER_KEY_TAB_USERNAME, parameterPojo.getLoginUserKeytabUsername());
        properties.put(LOGIN_USER_KEY_TAB_PATH, parameterPojo.getLoginUserKeytabPath());
        properties.put(YARN_APPLICATION_STATUS_ADDRESS, parameterPojo.getYarnApplicationStatusAddress());
        properties.put(YARN_RESOURCEMANAGER_HA_RM_IDS, parameterPojo.getYarnResourcemanagerHaRmIds());
        properties.put(FS_DEFAULTFS, parameterPojo.getFsDefaultFS());
        properties.put(HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT, parameterPojo.getResourceManagerHttpaddressPort());
        properties.put(HDFS_ROOT_USER, parameterPojo.getHdfsRootUser());
        properties.put(HADOOP_HOME, parameterPojo.getHadoopHomeDir());
        if (null == hdfsClient) {
            hdfsClient = new HdfsClient(properties);
        }
        return hdfsClient;
    }

    public synchronized static void close(){
        if (null != hdfsClient) {
            try {
                hdfsClient.close();
                hdfsClient=null;
            } catch (IOException e) {
                log.error("close cause Exception",e);
            }
        }
    }

//    public static void main(String[] args) throws  Exception {
//        //DefaultHdfsClient.getHdfsClient().appendToFileByWriteChars("/tmp/zyq/","test3.txt",100,"\n20000000000000\n\n3000000000000000000000000000");
//       // System.out.println( "============"+DefaultHdfsClient.getHdfsClient().getFSDataInputStreamByReadLine("/tmp/zyq/","test3.txt"));
//        DefaultHdfsClient.getHdfsClient().listFiles("/tmp",false).forEach(str->{
//            System.out.println(str);
//        });
//        DefaultHdfsClient.getHdfsClient().close();
//    }
}
