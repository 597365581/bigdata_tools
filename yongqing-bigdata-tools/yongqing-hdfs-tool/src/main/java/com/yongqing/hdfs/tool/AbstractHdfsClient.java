package com.yongqing.hdfs.tool;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yongqing.common.bigdata.tool.BigDataHttpClient;
import com.yongqing.hdfs.tool.constant.Constants;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.yongqing.hdfs.tool.constant.Constants.*;

/**
 */
@Log4j2
public abstract class AbstractHdfsClient implements Clinet {
    private FileSystem fs;
    private volatile Configuration configuration;
    private String hdfsUser;
    private Properties hdfsProperties;

    protected AbstractHdfsClient() {
        super();
        hdfsProperties = new Properties();
        init();
    }

    protected AbstractHdfsClient(Properties hdfsProperties) {
        super();
        this.hdfsProperties = hdfsProperties;
        init();
    }

    @Override
    public synchronized void close() throws IOException {
        if (null != fs) {
            try {
                fs.close();
            } catch (IOException e) {
                log.error("Close HDFS FileSystem instance failed !", e);
                throw new IOException("Close HDFS FileSystem instance failed", e);
            }
        }
    }

    @Override
    public FileSystem getFileSystem() {

        return fs;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    private void init() {
        synchronized (this) {
            if (null == configuration) {
                // 获取资源文件
                InputStream is = AbstractHdfsClient.class.getClassLoader().getResourceAsStream("hdfs.properties");
                //属性列表
                Properties prop = new Properties();
                try {
                    prop.load(is);
                } catch (IOException e) {
                    log.error("read hdfs.properties fail", e);
                } finally {
                    if (null != is) {
                        try {
                            is.close();
                        } catch (IOException e) {
                            log.error("close hdfs.properties fail", e);
                        }
                    }
                }
                try {
                    configuration = new Configuration();
                    //System.getProperties().setProperty("HADOOP_HOME", "/");
                    System.getProperties().setProperty(Constants.HADOOP_HOME, null == hdfsProperties.getProperty(Constants.HADOOP_HOME) ? prop.getProperty(Constants.HADOOP_HOME) : hdfsProperties.getProperty(Constants.HADOOP_HOME));
                    String state = null == hdfsProperties.getProperty(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE) ? prop.getProperty(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE) : hdfsProperties.getProperty(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE);
                    if (null != state && "true".equals(state)) {
                        System.getProperties().setProperty(Constants.JAVA_SECURITY_KRB5_CONF, null == hdfsProperties.getProperty(Constants.JAVA_SECURITY_KRB5_CONF_PATH) ? prop.getProperty(Constants.JAVA_SECURITY_KRB5_CONF_PATH) : hdfsProperties.getProperty(Constants.JAVA_SECURITY_KRB5_CONF_PATH));
                        configuration.set(Constants.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
                        UserGroupInformation.setConfiguration(configuration);
                        UserGroupInformation.loginUserFromKeytab(null == hdfsProperties.getProperty(Constants.LOGIN_USER_KEY_TAB_USERNAME) ? prop.getProperty(Constants.LOGIN_USER_KEY_TAB_USERNAME) : hdfsProperties.getProperty(Constants.LOGIN_USER_KEY_TAB_USERNAME), null == hdfsProperties.getProperty(Constants.LOGIN_USER_KEY_TAB_PATH) ? prop.getProperty(Constants.LOGIN_USER_KEY_TAB_PATH) : hdfsProperties.getProperty(Constants.LOGIN_USER_KEY_TAB_PATH));
                    }

                    String defaultFS = configuration.get(FS_DEFAULTFS);
                    if (defaultFS.startsWith("file")) {
                        String defaultFSProp = null == hdfsProperties.getProperty(FS_DEFAULTFS) ? prop.getProperty(FS_DEFAULTFS) : hdfsProperties.getProperty(FS_DEFAULTFS);
                        if (StringUtils.isNotBlank(defaultFSProp)) {
                            Map<String, String> fsRelatedProps = getPrefixedProperties("fs.", prop);
                            configuration.set(FS_DEFAULTFS, defaultFSProp);
                            fsRelatedProps.entrySet().stream().forEach(entry -> configuration.set(entry.getKey(), entry.getValue()));
                        } else {
                            log.error("property:{} can not to be empty, please set!");
                            throw new RuntimeException("property:{} can not to be empty, please set!");
                        }
                    } else {
                        log.info("get property:{} -> {}, from core-site.xml hdfs-site.xml ", FS_DEFAULTFS, defaultFS);
                    }
                    if (null == hdfsUser) {
                        hdfsUser = null == hdfsProperties.getProperty(Constants.HDFS_ROOT_USER) ? prop.getProperty(Constants.HDFS_ROOT_USER) : hdfsProperties.getProperty(Constants.HDFS_ROOT_USER);
                    }

                    if (null == fs) {

                        if (StringUtils.isNotEmpty(hdfsUser)) {
                            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
                            ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
                                @Override
                                public Boolean run() throws Exception {
                                    fs = FileSystem.get(configuration);
                                    return true;
                                }
                            });
                        } else {
                            log.warn("hdfs.root.user is not set value!");
                            fs = FileSystem.get(configuration);
                        }
                    }

                    String rmHaIds = null == hdfsProperties.getProperty(Constants.YARN_RESOURCEMANAGER_HA_RM_IDS) ? prop.getProperty(Constants.YARN_RESOURCEMANAGER_HA_RM_IDS) : hdfsProperties.getProperty(Constants.YARN_RESOURCEMANAGER_HA_RM_IDS);
                    String appAddress = null == hdfsProperties.getProperty(Constants.YARN_APPLICATION_STATUS_ADDRESS) ? prop.getProperty(Constants.YARN_APPLICATION_STATUS_ADDRESS) : hdfsProperties.getProperty(Constants.YARN_APPLICATION_STATUS_ADDRESS);
                    if (!StringUtils.isEmpty(rmHaIds)) {
                        appAddress = getAppAddress(appAddress, rmHaIds, prop);
                        log.info("appAddress:{}", appAddress);
                    }
                    configuration.set(Constants.YARN_APPLICATION_STATUS_ADDRESS, appAddress);
                } catch (Exception e) {
                    log.error("init  hdfs cause Exception", e);
                }
            }
        }
    }

    /**
     * getAppAddress(仅支持init调用)
     *
     * @param appAddress
     * @param rmHa
     * @return
     */
    private String getAppAddress(String appAddress, String rmHa, Properties prop) {

        //get active ResourceManager
        String activeRM = getAcitveRMName(rmHa, prop);

        String[] split1 = appAddress.split(DOUBLE_SLASH);

        if (split1.length != 2) {
            return null;
        }

        String start = split1[0] + DOUBLE_SLASH;
        String[] split2 = split1[1].split(COLON);

        if (split2.length != 2) {
            return null;
        }

        String end = COLON + split2[1];

        return start + activeRM + end;
    }

    //(仅支持init调用)
    private String getAcitveRMName(String rmIds, Properties prop) {

        String[] rmIdArr = rmIds.split(COMMA);

        String port = null == hdfsProperties.getProperty(HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT) ? prop.getProperty(HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT) : hdfsProperties.getProperty(HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT);
        int activeResourceManagerPort = Integer.valueOf(StringUtils.isEmpty(port) ? "8088" : port);

        String yarnUrl = "http://%s:" + activeResourceManagerPort + "/ws/v1/cluster/info";

        String state = null;
        try {
            /**
             * send http get request to rm1
             */
            state = getRMState(String.format(yarnUrl, rmIdArr[0]));

            if (HADOOP_RM_STATE_ACTIVE.equals(state)) {
                return rmIdArr[0];
            } else if (HADOOP_RM_STATE_STANDBY.equals(state)) {
                state = getRMState(String.format(yarnUrl, rmIdArr[1]));
                if (HADOOP_RM_STATE_ACTIVE.equals(state)) {
                    return rmIdArr[1];
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            state = getRMState(String.format(yarnUrl, rmIdArr[1]));
            if (HADOOP_RM_STATE_ACTIVE.equals(state)) {
                return rmIdArr[0];
            }
        }
        return null;
    }

    /**
     * get ResourceManager state (仅支持init调用)
     *
     * @param url
     * @return
     */
    private String getRMState(String url) {

        String retStr = BigDataHttpClient.doGet(url);

        if (StringUtils.isEmpty(retStr)) {
            return null;
        }
        JsonParser jsonParser = new JsonParser();
        //to json
        JsonObject jsonObject = jsonParser.parse(retStr).getAsJsonObject();


        //get ResourceManager state
        String state = jsonObject.getAsJsonObject("clusterInfo").get("haState").getAsString();
        return state;
    }

    //(仅支持init调用)
    private Map<String, String> getPrefixedProperties(String prefix, Properties prop) {
        Map<String, String> matchedProperties = new HashMap<>();
        for (String propName : prop.stringPropertyNames()) {
            if (propName.startsWith(prefix)) {
                matchedProperties.put(propName, null == hdfsProperties.getProperty(propName) ? prop.getProperty(propName) : hdfsProperties.getProperty(propName));
            }
        }
        return matchedProperties;
    }

}
