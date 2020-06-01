package com.yongqing.processor.log.bean;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class SqlLog {
    //日志ID  必填，系统统一生成获取
    private String sqlLogId;
    //采集标志 必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为 sqlCollection
    private String collectionSign = "sqlCollection";
    //sql日志打印时间 必填，系统统一生成获取(sql日志打印时间)
    private String sqlLogTime;
    //自定义字段这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ "key1":"value1","key2":"value2" }
    private Map<String, Object> customizeField;
    //业务id  必填，业务ID（每个业务不可重复，建议未来统一生成和分配，在接入统一采集）
    private String sqlBusinessId;
    //业务类型 必填，业务类型（统一定义）
    private String sqlBusinessType;
    //系统名称  必填，业务系统(系统的唯一英文简称)
    private String sqlBusinessSystem;
    //业务名称 必填，业务名称
    private String sqlBusinessName;
    //0 原始未区分（增删改查都包含）1 查询 2 插入 3 修改 4 删除
    private String sqlType;
    private List<String> sqlList;

    public String getSqlLogId() {
        return sqlLogId;
    }

    public void setSqlLogId(String sqlLogId) {
        this.sqlLogId = sqlLogId;
    }

    public String getCollectionSign() {
        return collectionSign;
    }

    public void setCollectionSign(String collectionSign) {
        this.collectionSign = collectionSign;
    }

    public String getSqlLogTime() {
        return sqlLogTime;
    }

    public void setSqlLogTime(String sqlLogTime) {
        this.sqlLogTime = sqlLogTime;
    }

    public Map<String, Object> getCustomizeField() {
        return customizeField;
    }

    public void setCustomizeField(Map<String, Object> customizeField) {
        this.customizeField = customizeField;
    }

    public String getSqlBusinessId() {
        return sqlBusinessId;
    }

    public void setSqlBusinessId(String sqlBusinessId) {
        this.sqlBusinessId = sqlBusinessId;
    }

    public String getSqlBusinessType() {
        return sqlBusinessType;
    }

    public void setSqlBusinessType(String sqlBusinessType) {
        this.sqlBusinessType = sqlBusinessType;
    }

    public String getSqlBusinessSystem() {
        return sqlBusinessSystem;
    }

    public void setSqlBusinessSystem(String sqlBusinessSystem) {
        this.sqlBusinessSystem = sqlBusinessSystem;
    }

    public String getSqlBusinessName() {
        return sqlBusinessName;
    }

    public void setSqlBusinessName(String sqlBusinessName) {
        this.sqlBusinessName = sqlBusinessName;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    @Override
    public String toString() {
        return "SqlLog{" +
                "sqlLogId='" + sqlLogId + '\'' +
                ", collectionSign='" + collectionSign + '\'' +
                ", sqlLogTime='" + sqlLogTime + '\'' +
                ", customizeField=" + customizeField +
                ", sqlBusinessId='" + sqlBusinessId + '\'' +
                ", sqlBusinessType='" + sqlBusinessType + '\'' +
                ", sqlBusinessSystem='" + sqlBusinessSystem + '\'' +
                ", sqlBusinessName='" + sqlBusinessName + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", sqlList=" + sqlList +
                '}';
    }
}
