package com.yongqing.log.flume.sink.bean;

/**
 *
 */
public class DataBaseConfig {
    private String businessId;
    private String databaseName;
    private String tableName;
    private String primaryKeyField;
    private String filterFields;

    public String getFilterFields() {
        return filterFields;
    }

    public void setFilterFields(String filterFields) {
        this.filterFields = filterFields;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKeyField() {
        return primaryKeyField;
    }

    public void setPrimaryKeyField(String primaryKeyField) {
        this.primaryKeyField = primaryKeyField;
    }

    @Override
    public String toString() {
        return "DataBaseConfig{" +
                "businessId='" + businessId + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", primaryKeyField='" + primaryKeyField + '\'' +
                ", filterFields='" + filterFields + '\'' +
                '}';
    }
}
