package com.yongqing.hdfs.tool.constant;

/**
 *
 */
public interface Constants {



    /**
     * COLON :
     */
     String COLON = ":";

    /**
     * SINGLE_SLASH /
     */
     String SINGLE_SLASH = "/";

    /**
     * DOUBLE_SLASH //
     */
    String DOUBLE_SLASH = "//";

    /**
     * SEMICOLON ;
     */
   String SEMICOLON = ";";

    /**
     * comma ,
     */
     String COMMA = ",";

    /**
     * hadoop configuration
     */
     String HADOOP_RM_STATE_ACTIVE = "ACTIVE";

    String HADOOP_RM_STATE_STANDBY = "STANDBY";

     String HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT = "resource.manager.httpaddress.port";

    /**
     * fs.defaultFS
     */
     String FS_DEFAULTFS = "fs.defaultFS";

    /**
     * yarn.resourcemanager.ha.rm.idsfs.defaultFS
     */
   String YARN_RESOURCEMANAGER_HA_RM_IDS = "yarn.resourcemanager.ha.rm.ids";

    /**
     * yarn.application.status.address
     */
    String YARN_APPLICATION_STATUS_ADDRESS = "yarn.application.status.address";

    /**
     * hdfs configuration
     * hdfs.root.user
     */
    String HDFS_ROOT_USER = "hdfs.root.user";
    /**
     * loginUserFromKeytab path
     */
     String LOGIN_USER_KEY_TAB_PATH = "login.user.keytab.path";
    /**
     * java.security.krb5.conf
     */
     String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    /**
     * java.security.krb5.conf.path
     */
    String JAVA_SECURITY_KRB5_CONF_PATH = "java.security.krb5.conf.path";

    /**
     * hadoop.security.authentication
     */
    String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    /**
     * hadoop.security.authentication
     */
    String HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE = "hadoop.security.authentication.startup.state";


    /**
     * loginUserFromKeytab user
     */
    String LOGIN_USER_KEY_TAB_USERNAME = "login.user.keytab.username";

    String HADOOP_HOME="hadoop.home.dir";

}
