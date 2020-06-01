package com.yongqing.hdfs.tool.pojo;

/**
 *
 */
public class ParameterPojo {

    private String javaSecurityKrb5Conf;
    private String javaSecurityKrb5ConfPath;
    private String hadoopSecurityAuthentication;
    private String hadoopSecurityAuthenticationStartupState;
    private String loginUserKeytabUsername;
    private String loginUserKeytabPath;
    private String yarnApplicationStatusAddress;
    private String yarnResourcemanagerHaRmIds;
    private String fsDefaultFS;
    private String resourceManagerHttpaddressPort;
    private String hdfsRootUser;
    private String hadoopHomeDir;

    public String getJavaSecurityKrb5Conf() {
        return javaSecurityKrb5Conf;
    }

    public void setJavaSecurityKrb5Conf(String javaSecurityKrb5Conf) {
        this.javaSecurityKrb5Conf = javaSecurityKrb5Conf;
    }

    public String getJavaSecurityKrb5ConfPath() {
        return javaSecurityKrb5ConfPath;
    }

    public void setJavaSecurityKrb5ConfPath(String javaSecurityKrb5ConfPath) {
        this.javaSecurityKrb5ConfPath = javaSecurityKrb5ConfPath;
    }

    public String getHadoopSecurityAuthentication() {
        return hadoopSecurityAuthentication;
    }

    public void setHadoopSecurityAuthentication(String hadoopSecurityAuthentication) {
        this.hadoopSecurityAuthentication = hadoopSecurityAuthentication;
    }

    public String getHadoopSecurityAuthenticationStartupState() {
        return hadoopSecurityAuthenticationStartupState;
    }

    public void setHadoopSecurityAuthenticationStartupState(String hadoopSecurityAuthenticationStartupState) {
        this.hadoopSecurityAuthenticationStartupState = hadoopSecurityAuthenticationStartupState;
    }

    public String getLoginUserKeytabUsername() {
        return loginUserKeytabUsername;
    }

    public void setLoginUserKeytabUsername(String loginUserKeytabUsername) {
        this.loginUserKeytabUsername = loginUserKeytabUsername;
    }

    public String getLoginUserKeytabPath() {
        return loginUserKeytabPath;
    }

    public void setLoginUserKeytabPath(String loginUserKeytabPath) {
        this.loginUserKeytabPath = loginUserKeytabPath;
    }

    public String getYarnApplicationStatusAddress() {
        return yarnApplicationStatusAddress;
    }

    public void setYarnApplicationStatusAddress(String yarnApplicationStatusAddress) {
        this.yarnApplicationStatusAddress = yarnApplicationStatusAddress;
    }

    public String getYarnResourcemanagerHaRmIds() {
        return yarnResourcemanagerHaRmIds;
    }

    public void setYarnResourcemanagerHaRmIds(String yarnResourcemanagerHaRmIds) {
        this.yarnResourcemanagerHaRmIds = yarnResourcemanagerHaRmIds;
    }

    public String getFsDefaultFS() {
        return fsDefaultFS;
    }

    public void setFsDefaultFS(String fsDefaultFS) {
        this.fsDefaultFS = fsDefaultFS;
    }

    public String getResourceManagerHttpaddressPort() {
        return resourceManagerHttpaddressPort;
    }

    public void setResourceManagerHttpaddressPort(String resourceManagerHttpaddressPort) {
        this.resourceManagerHttpaddressPort = resourceManagerHttpaddressPort;
    }

    public String getHdfsRootUser() {
        return hdfsRootUser;
    }

    public void setHdfsRootUser(String hdfsRootUser) {
        this.hdfsRootUser = hdfsRootUser;
    }

    public String getHadoopHomeDir() {
        return hadoopHomeDir;
    }

    public void setHadoopHomeDir(String hadoopHomeDir) {
        this.hadoopHomeDir = hadoopHomeDir;
    }

    @Override
    public String toString() {
        return "ParameterPojo{" +
                "javaSecurityKrb5Conf='" + javaSecurityKrb5Conf + '\'' +
                ", javaSecurityKrb5ConfPath='" + javaSecurityKrb5ConfPath + '\'' +
                ", hadoopSecurityAuthentication='" + hadoopSecurityAuthentication + '\'' +
                ", hadoopSecurityAuthenticationStartupState='" + hadoopSecurityAuthenticationStartupState + '\'' +
                ", loginUserKeytabUsername='" + loginUserKeytabUsername + '\'' +
                ", loginUserKeytabPath='" + loginUserKeytabPath + '\'' +
                ", yarnApplicationStatusAddress='" + yarnApplicationStatusAddress + '\'' +
                ", yarnResourcemanagerHaRmIds='" + yarnResourcemanagerHaRmIds + '\'' +
                ", fsDefaultFS='" + fsDefaultFS + '\'' +
                ", resourceManagerHttpaddressPort='" + resourceManagerHttpaddressPort + '\'' +
                ", hdfsRootUser='" + hdfsRootUser + '\'' +
                ", hadoopHomeDir='" + hadoopHomeDir + '\'' +
                '}';
    }

}
