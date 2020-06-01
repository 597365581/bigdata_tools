package com.yongqing.processor.log.bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MysqlBinLog {
    private List<Map<String, Object>> data;
    private String database;
    private Long es;
    private Long id;
    private boolean isDdl;
    private Map<String, Object> mysqlType;
    private List<Map<String, Object>> old;
    private List<String> pkNames;
    private String sql;
    private Map<String, Object> sqlType;
    private String table;
    private String type;
    private Long ts;

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    public Map<String, Object> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, Object> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, Object> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Object> sqlType) {
        this.sqlType = sqlType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void addPkName(String pkName) {
        if (this.pkNames == null) {
            this.pkNames = new ArrayList<>();
        }
        this.pkNames.add(pkName);
    }

    @Override
    public String toString() {
        return "MysqlBinLog{" +
                "data=" + data +
                ", database='" + database + '\'' +
                ", es=" + es +
                ", id=" + id +
                ", isDdl=" + isDdl +
                ", mysqlType=" + mysqlType +
                ", old=" + old +
                ", pkNames=" + pkNames +
                ", sql='" + sql + '\'' +
                ", sqlType=" + sqlType +
                ", table='" + table + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
