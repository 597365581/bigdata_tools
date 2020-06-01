package com.yongqing.hive.tool.pojo;

/**
 *
 */
public class ParameterPojo {

    private String metaStoreUri;
    private String proxyUser;
    private String database;
    private String table;
    private String hivePartition;
    private Integer hiveTxnsPerBatchAsk;
    private Integer batchSize;
    private Integer idleTimeout;
    private Integer callTimeout;
    private Integer heartBeatInterval;
    private Integer maxOpenConnections;
    private Boolean autoCreatePartitions;
    private Boolean useLocalTime;
    private String tzName;
    private Boolean needRounding;
    private Integer roundUnit;
    private Integer roundValue;
    private FieldDealPojo fieldDealPojo;


    public String getMetaStoreUri() {
        return metaStoreUri;
    }

    public void setMetaStoreUri(String metaStoreUri) {
        this.metaStoreUri = metaStoreUri;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getHivePartition() {
        return hivePartition;
    }

    public void setHivePartition(String hivePartition) {
        this.hivePartition = hivePartition;
    }

    public Integer getHiveTxnsPerBatchAsk() {
        return hiveTxnsPerBatchAsk;
    }

    public void setHiveTxnsPerBatchAsk(Integer hiveTxnsPerBatchAsk) {
        this.hiveTxnsPerBatchAsk = hiveTxnsPerBatchAsk;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Integer idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Integer getCallTimeout() {
        return callTimeout;
    }

    public void setCallTimeout(Integer callTimeout) {
        this.callTimeout = callTimeout;
    }

    public Integer getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public void setHeartBeatInterval(Integer heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }

    public Integer getMaxOpenConnections() {
        return maxOpenConnections;
    }

    public void setMaxOpenConnections(Integer maxOpenConnections) {
        this.maxOpenConnections = maxOpenConnections;
    }

    public Boolean getAutoCreatePartitions() {
        return autoCreatePartitions;
    }

    public void setAutoCreatePartitions(Boolean autoCreatePartitions) {
        this.autoCreatePartitions = autoCreatePartitions;
    }

    public Boolean getUseLocalTime() {
        return useLocalTime;
    }

    public void setUseLocalTime(Boolean useLocalTime) {
        this.useLocalTime = useLocalTime;
    }

    public String getTzName() {
        return tzName;
    }

    public void setTzName(String tzName) {
        this.tzName = tzName;
    }

    public Boolean getNeedRounding() {
        return needRounding;
    }

    public void setNeedRounding(Boolean needRounding) {
        this.needRounding = needRounding;
    }

    public Integer getRoundUnit() {
        return roundUnit;
    }

    public void setRoundUnit(Integer roundUnit) {
        this.roundUnit = roundUnit;
    }

    public Integer getRoundValue() {
        return roundValue;
    }

    public void setRoundValue(Integer roundValue) {
        this.roundValue = roundValue;
    }

    public FieldDealPojo getFieldDealPojo() {
        return fieldDealPojo;
    }

    public void setFieldDealPojo(FieldDealPojo fieldDealPojo) {
        this.fieldDealPojo = fieldDealPojo;
    }

    @Override
    public String toString() {
        return "ParameterPojo{" +
                "metaStoreUri='" + metaStoreUri + '\'' +
                ", proxyUser='" + proxyUser + '\'' +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", hivePartition='" + hivePartition + '\'' +
                ", hiveTxnsPerBatchAsk=" + hiveTxnsPerBatchAsk +
                ", batchSize=" + batchSize +
                ", idleTimeout=" + idleTimeout +
                ", callTimeout=" + callTimeout +
                ", heartBeatInterval=" + heartBeatInterval +
                ", maxOpenConnections=" + maxOpenConnections +
                ", autoCreatePartitions=" + autoCreatePartitions +
                ", useLocalTime=" + useLocalTime +
                ", tzName='" + tzName + '\'' +
                ", needRounding=" + needRounding +
                ", roundUnit=" + roundUnit +
                ", roundValue=" + roundValue +
                ", fieldDealPojo=" + fieldDealPojo +
                '}';
    }
}
