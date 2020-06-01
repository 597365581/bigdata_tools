package com.yongqing.hive.tool.pojo;

/**
 *
 */
public class FieldDealPojo {
    private String serdeSeparator;
    private String delimiter;
    private String fieldNames;

    public String getSerdeSeparator() {
        return serdeSeparator;
    }

    public void setSerdeSeparator(String serdeSeparator) {
        this.serdeSeparator = serdeSeparator;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String toString() {
        return "FieldDealPojo{" +
                "serdeSeparator=" + serdeSeparator +
                ", delimiter='" + delimiter + '\'' +
                ", fieldNames='" + fieldNames + '\'' +
                '}';
    }
}
