package com.yongqing.sql.analyse.bean;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class SqlOutput {

    private String sql;
    //query update delete all insert
    private String sqlType;

    private List<Map<String, Object>> sqlResult;

    private String execStatus;

    private String execMessage;
    private Integer effectRowNum;

    public Integer getEffectRowNum() {
        return effectRowNum;
    }

    public void setEffectRowNum(Integer effectRowNum) {
        this.effectRowNum = effectRowNum;
    }

    public String getExecStatus() {
        return execStatus;
    }

    public void setExecStatus(String execStatus) {
        this.execStatus = execStatus;
    }

    public String getExecMessage() {
        return execMessage;
    }

    public void setExecMessage(String execMessage) {
        this.execMessage = execMessage;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public List<Map<String, Object>> getSqlResult() {
        return sqlResult;
    }

    public void setSqlResult(List<Map<String, Object>> sqlResult) {
        this.sqlResult = sqlResult;
    }

    @Override
    public String toString() {
        return "SqlOutput{" +
                "sql='" + sql + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", sqlResult=" + sqlResult +
                ", execStatus='" + execStatus + '\'' +
                ", execMessage='" + execMessage + '\'' +
                ", effectRowNum=" + effectRowNum +
                '}';
    }
}
